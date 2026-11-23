# LAN test environment - CLIENT machine (any Windows PC, nothing to install).
# Copy this single file to the client PC and run, e.g.:
#
#   powershell -ExecutionPolicy Bypass -File .\lan-client.ps1 -Server 192.168.1.20
#   powershell -ExecutionPolicy Bypass -File .\lan-client.ps1 -Server 192.168.1.20 -Scenario burst -Count 100
#   powershell -ExecutionPolicy Bypass -File .\lan-client.ps1 -Server 192.168.1.20 -Scenario slow -Count 5
#
# Scenarios:
#   mixed  (default) one request every -IntervalMs, rotating job types:
#          simulate_delay, text_transform, json_format, cpu_compute, plus one
#          deliberately invalid request (expect 422) to show error handling.
#   slow   only simulate_delay jobs of -DelaySeconds each (queueing is visible).
#   burst  -Count requests sent at the SAME moment (backpressure: 429 / 503).
#
# Every request carries "x-client-name: <Name>", so the server logs show which
# PC sent it (together with the PC's IP address in client_addr).
param(
    [Parameter(Mandatory = $true)]
    [string]$Server,                       # 192.168.1.20  or  192.168.1.20:8080  or  http://...
    [string]$Name = $env:COMPUTERNAME,
    [ValidateSet('mixed', 'slow', 'burst')]
    [string]$Scenario = 'mixed',
    [int]$Count = 20,
    [int]$IntervalMs = 500,
    [int]$DelaySeconds = 5,
    [switch]$NoWait                         # do not poll jobs until they finish
)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Net.Http

if ($Server -notmatch '^https?://') { $Server = "http://$Server" }
if ($Server -notmatch ':\d+$') { $Server = "${Server}:8080" }
$Base = $Server.TrimEnd('/')

$http = New-Object System.Net.Http.HttpClient
$http.Timeout = [TimeSpan]::FromSeconds(30)
$http.DefaultRequestHeaders.Add('x-client-name', $Name)

function Now { (Get-Date).ToString('HH:mm:ss.fff') }

function ConvertTo-Body($jobType, $payload) {
    $json = @{ job_type = $jobType; payload = $payload } | ConvertTo-Json -Depth 6 -Compress
    New-Object System.Net.Http.StringContent($json, [Text.Encoding]::UTF8, 'application/json')
}

function Read-Response($resp) {
    $text = $resp.Content.ReadAsStringAsync().Result
    $obj = $null
    try { $obj = $text | ConvertFrom-Json } catch {}
    [pscustomobject]@{ Code = [int]$resp.StatusCode; Body = $obj; Raw = $text }
}

function Get-JobSpec([int]$i) {
    switch ($Scenario) {
        'slow' { return @('simulate_delay', @{ seconds = $DelaySeconds }) }
        'burst' { return @('simulate_delay', @{ duration_ms = 200 }) }
    }
    switch ($i % 5) {
        0 { @('simulate_delay', @{ seconds = 2 }) }
        1 { @('text_transform', @{ text = "merhaba from $Name #$i"; operation = 'uppercase' }) }
        2 { @('json_format', @{ input = '{"b":1,"a":[1,2,3]}'; indent = 2; sort_keys = $true }) }
        3 { @('cpu_compute', @{ from = 0; to = 2000000; compare_sequential = $false }) }
        4 { @('simulate_delay', @{ seconds = -1 }) }   # invalid on purpose -> 422
    }
}

$stats = @{}
$accepted = New-Object System.Collections.Generic.List[string]

function Write-Result($i, $jobType, $r) {
    $key = "$($r.Code)"
    if ($stats.ContainsKey($key)) { $stats[$key]++ } else { $stats[$key] = 1 }
    if ($r.Code -eq 202) {
        $id = $r.Body.job.job_id
        $accepted.Add($id)
        Write-Host ("{0} #{1,-3} {2,-15} -> 202 accepted  job={3}  status={4}" -f (Now), $i, $jobType, $id, $r.Body.job.status) -ForegroundColor Green
    } else {
        $code = ''
        if ($r.Body -and $r.Body.error) { $code = "$($r.Body.error.code): $($r.Body.error.message)" } else { $code = $r.Raw }
        Write-Host ("{0} #{1,-3} {2,-15} -> {3}  {4}" -f (Now), $i, $jobType, $r.Code, $code) -ForegroundColor Yellow
    }
}

# ---- 1. reachability -------------------------------------------------------
Write-Host "client '$Name' -> $Base  scenario=$Scenario count=$Count" -ForegroundColor Cyan
try {
    $ready = Read-Response ($http.GetAsync("$Base/api/v1/health/ready").Result)
} catch {
    Write-Host "Cannot reach $Base : $($_.Exception.InnerException.Message)" -ForegroundColor Red
    Write-Host 'Check: same network? server started with lan-server.ps1 -Role gateway? firewall rule added?' -ForegroundColor Red
    exit 1
}
Write-Host ("{0} health/ready -> {1} {2}" -f (Now), $ready.Code, $ready.Raw)
if ($ready.Code -ne 200) { Write-Host 'Server is up but not ready (worker/database?). Continuing anyway.' -ForegroundColor Yellow }

# ---- 2. send ---------------------------------------------------------------
$started = Get-Date
if ($Scenario -eq 'burst') {
    $tasks = @()
    $types = @()
    for ($i = 0; $i -lt $Count; $i++) {
        $spec = Get-JobSpec $i
        $types += $spec[0]
        $tasks += $http.PostAsync("$Base/api/v1/jobs", (ConvertTo-Body $spec[0] $spec[1]))
    }
    for ($i = 0; $i -lt $tasks.Count; $i++) {
        try {
            Write-Result $i $types[$i] (Read-Response $tasks[$i].Result)
        } catch {
            if ($stats.ContainsKey('error')) { $stats['error']++ } else { $stats['error'] = 1 }
            Write-Host ("{0} #{1,-3} transport error: {2}" -f (Now), $i, $_.Exception.InnerException.Message) -ForegroundColor Red
        }
    }
} else {
    for ($i = 0; $i -lt $Count; $i++) {
        $spec = Get-JobSpec $i
        try {
            Write-Result $i $spec[0] (Read-Response ($http.PostAsync("$Base/api/v1/jobs", (ConvertTo-Body $spec[0] $spec[1]))).Result)
        } catch {
            if ($stats.ContainsKey('error')) { $stats['error']++ } else { $stats['error'] = 1 }
            Write-Host ("{0} #{1,-3} transport error: {2}" -f (Now), $i, $_.Exception.InnerException.Message) -ForegroundColor Red
        }
        if ($i -lt $Count - 1) { Start-Sleep -Milliseconds $IntervalMs }
    }
}
$sendSecs = ((Get-Date) - $started).TotalSeconds

Write-Host ''
Write-Host ("sent {0} requests in {1:N1}s; responses by HTTP code:" -f $Count, $sendSecs) -ForegroundColor Cyan
$stats.GetEnumerator() | Sort-Object Name | ForEach-Object { Write-Host ("  {0,-6} {1}" -f $_.Name, $_.Value) }

# ---- 3. follow accepted jobs until terminal ---------------------------------
if ($NoWait -or $accepted.Count -eq 0) { exit 0 }
Write-Host "`nwaiting for $($accepted.Count) accepted jobs to finish (Ctrl-C to stop)..." -ForegroundColor Cyan
$terminal = @('completed', 'failed', 'cancelled')
$final = @{}
$deadline = (Get-Date).AddMinutes(15)
while ($final.Count -lt $accepted.Count -and (Get-Date) -lt $deadline) {
    foreach ($id in $accepted) {
        if ($final.ContainsKey($id)) { continue }
        try {
            $r = Read-Response ($http.GetAsync("$Base/api/v1/jobs/$id").Result)
            $st = "$($r.Body.status)"
            if ($terminal -contains $st) {
                $final[$id] = $st
                $took = ''
                if ($r.Body.started_at -and $r.Body.finished_at) {
                    $took = '{0:N0} ms' -f (([datetime]$r.Body.finished_at) - ([datetime]$r.Body.started_at)).TotalMilliseconds
                }
                Write-Host ("{0} job {1} {2,-15} -> {3}  attempts={4} {5}" -f (Now), $id, $r.Body.job_type, $st, $r.Body.attempts, $took)
            }
        } catch {}
    }
    if ($final.Count -lt $accepted.Count) { Start-Sleep -Seconds 1 }
}
Write-Host ''
Write-Host 'final job states:' -ForegroundColor Cyan
$final.Values | Group-Object | ForEach-Object { Write-Host ("  {0,-14} {1}" -f $_.Name, $_.Count) }
$pending = $accepted.Count - $final.Count
if ($pending -gt 0) { Write-Host "  still running  $pending" -ForegroundColor Yellow }
