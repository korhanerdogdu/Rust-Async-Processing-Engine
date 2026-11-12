//! Durable artifact directory with path-traversal protection and atomic
//! finalization.
//!
//! Write protocol (crash-safe):
//! 1. the producer writes to `<root>/tmp/<job>-<attempt>-<rand>.part`
//! 2. [`ArtifactStore::finalize`] validates, hashes and `fsync`s the temp file
//! 3. it is `rename`d to `<root>/<job_id>.<ext>` – atomic on the same filesystem
//! 4. only then is the metadata committed to PostgreSQL (`completed`)
//!
//! A crash before step 3 leaves only a `.part` file (deleted at next startup by
//! [`ArtifactStore::cleanup_tmp`]). A crash between 3 and 4 leaves a complete
//! file whose job is still `processing`; recovery re-runs the job and the
//! rename simply replaces the file (at-least-once, idempotent result path).

use std::io::{self, Read};
use std::path::{Path, PathBuf};

use engine_core::{ArtifactRef, JobId};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("invalid artifact path")]
    InvalidPath,
    #[error("artifact I/O error: {0}")]
    Io(#[from] io::Error),
}

impl ArtifactStore {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = std::path::absolute(root.into())?;
        std::fs::create_dir_all(root.join("tmp"))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    pub fn file_name(job_id: JobId, ext: &str) -> String {
        format!("{job_id}.{ext}")
    }

    /// Resolves a stored relative path. Only plain file names made of a safe
    /// character set are accepted: no separators, no `..`, no drive letters.
    pub fn resolve(&self, relative: &str) -> Result<PathBuf, ArtifactError> {
        let valid = !relative.is_empty()
            && relative.len() <= 128
            && !relative.starts_with('.')
            && !relative.contains("..")
            && relative
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
        if !valid {
            return Err(ArtifactError::InvalidPath);
        }
        Ok(self.root.join(relative))
    }

    /// **Blocking** (std::fs): hash + fsync + atomic rename. Call it from
    /// `spawn_blocking`, never directly on a Tokio worker.
    pub fn finalize_blocking(
        &self,
        tmp: &Path,
        job_id: JobId,
        ext: &str,
        content_type: &str,
    ) -> Result<ArtifactRef, ArtifactError> {
        let name = Self::file_name(job_id, ext);
        let dest = self.resolve(&name)?;
        let mut file = std::fs::File::open(tmp)?;
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        let mut size = 0u64;
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            size += n as u64;
            hasher.update(&buf[..n]);
        }
        drop(file);
        // fsync the data before making it visible under its final name.
        std::fs::OpenOptions::new()
            .write(true)
            .open(tmp)?
            .sync_all()?;
        std::fs::rename(tmp, &dest)?;
        Ok(ArtifactRef {
            relative_path: name,
            content_type: content_type.to_string(),
            size_bytes: size,
            sha256: hex::encode(hasher.finalize()),
        })
    }

    /// Removes leftovers of interrupted writes. Called once at worker startup.
    pub fn cleanup_tmp(&self) -> io::Result<usize> {
        let mut removed = 0;
        for entry in std::fs::read_dir(self.tmp_dir())? {
            let path = entry?.path();
            let res = if path.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            if res.is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Best-effort delete (e.g. a result produced for a job cancelled meanwhile).
    pub fn remove(&self, relative: &str) {
        if let Ok(path) = self.resolve(relative) {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_traversal_and_odd_names() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(dir.path()).unwrap();
        for bad in [
            "../etc/passwd",
            "..",
            "a/b.pdf",
            "a\\b.pdf",
            "C:x.pdf",
            ".hidden",
            "",
            "x y.pdf",
        ] {
            assert!(store.resolve(bad).is_err(), "{bad:?} must be rejected");
        }
        let id = JobId::generate();
        let ok = store.resolve(&ArtifactStore::file_name(id, "pdf")).unwrap();
        assert!(ok.starts_with(store.root()));
    }

    #[test]
    fn finalize_hashes_and_renames_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(dir.path()).unwrap();
        let tmp = store.tmp_dir().join("x.part");
        std::fs::write(&tmp, b"%PDF-1.7 hello").unwrap();
        let id = JobId::generate();
        let art = store
            .finalize_blocking(&tmp, id, "pdf", "application/pdf")
            .unwrap();
        assert!(!tmp.exists());
        assert_eq!(art.size_bytes, 14);
        assert_eq!(art.sha256.len(), 64);
        assert_eq!(
            std::fs::read(store.resolve(&art.relative_path).unwrap()).unwrap(),
            b"%PDF-1.7 hello"
        );
    }

    #[test]
    fn cleanup_removes_partial_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(dir.path()).unwrap();
        std::fs::write(store.tmp_dir().join("a.part"), b"x").unwrap();
        std::fs::create_dir(store.tmp_dir().join("workdir")).unwrap();
        assert_eq!(store.cleanup_tmp().unwrap(), 2);
    }
}
