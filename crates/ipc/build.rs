//! Generates Rust code for `proto/engine.proto` with tonic-prost-build.
//! Uses the vendored `protoc` binary so no system protobuf install is needed.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        let protoc = protoc_bin_vendored::protoc_bin_path()?;
        // SAFETY: build scripts are single-threaded at this point; nothing else
        // reads the environment concurrently.
        unsafe { std::env::set_var("PROTOC", protoc) };
    }
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let proto_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("../../proto");
    let include = protoc_bin_vendored::include_path()?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("engine_descriptor.bin"))
        .compile_protos(
            &[proto_dir.join("engine.proto")],
            &[proto_dir.clone(), include],
        )?;

    println!(
        "cargo:rerun-if-changed={}",
        proto_dir.join("engine.proto").display()
    );
    Ok(())
}
