# LAN test environment - SERVER machine.
#
#   .\scripts\lan-server.ps1 -Role info              # show this PC's LAN IPs + firewall state
#   .\scripts\lan-server.ps1 -Role firewall          # open TCP 8080/50051 (run PowerShell as Administrator)
#   .\scripts\lan-server.ps1 -Role firewall-remove   # remove those rules again (Administrator)
#   .\scripts\lan-server.ps1 -Role worker [-MaxActiveJobs 4]   # terminal 1
#   .\scripts\lan-server.ps1 -Role gateway                     # terminal 2 (listens on 0.0.0.0)
#
# Only the api-gateway is exposed to the network. The worker keeps listening on
# 127.0.0.1:50052 (internal gRPC) and the database / Jaeger stay local.
# Environment variables set here override .env for this process only, so .env
# does not need to be edited.
param(
    [ValidateSet('info', 'firewall', 'firewall-remove', 'worker', 'gateway')]
    [string]$Role = 'info',
    [int]$MaxActiveJobs = 0,
    [int]$QueueCapacity = 0,
    [ValidateSet('pretty', 'json')]
    [string]$LogFormat = 'pretty',
    [switch]$Release
)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

$RuleName = 'Rust Async Engine (LAN test)'
$Ports = @(8080, 50051)

function Show-LanAddresses {
    $ips = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object {
            $_.IPAddress -notlike '127.*' -and $_.IPAddress -notlike '169.254.*' -and
            $_.PrefixOrigin -ne 'WellKnown' -and
            # Hyper-V / WSL / Docker / VPN virtual adapters are not reachable from other PCs.
            $_.InterfaceAlias -notmatch '^(vEthernet|Loopback|VirtualBox|VMware)'
        }
    if (-not $ips) {
        Write-Host 'No LAN IPv4 address found. Is this PC connected to a network?' -ForegroundColor Red
        return
    }
    Write-Host 'Give one of these addresses to the client PCs:' -ForegroundColor Cyan
    foreach ($ip in $ips) {
        Write-Host ("  {0,-16} ({1})   REST: http://{0}:8080   gRPC: {0}:50051" -f $ip.IPAddress, $ip.InterfaceAlias)
    }
}

function Show-FirewallState {
    $rule = Get-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue
    if ($rule) {
        Write-Host "Firewall rule '$RuleName' exists (enabled: $($rule.Enabled))." -ForegroundColor Green
    } else {
        Write-Host "Firewall rule missing. Run as Administrator:  .\scripts\lan-server.ps1 -Role firewall" -ForegroundColor Yellow
    }
    $profiles = Get-NetConnectionProfile -ErrorAction SilentlyContinue
    foreach ($p in $profiles) {
        Write-Host ("  network '{0}' profile: {1}" -f $p.Name, $p.NetworkCategory)
    }
}

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    (New-Object Security.Principal.WindowsPrincipal $id).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Set-CommonEnv {
    $env:LOG_FORMAT = $LogFormat
}

$cargoProfile = @()
if ($Release) { $cargoProfile = @('--release') }

switch ($Role) {
    'info' {
        Show-LanAddresses
        Show-FirewallState
    }
    'firewall' {
        if (-not (Test-Admin)) { throw 'Run PowerShell as Administrator for this step.' }
        Get-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule
        # Profile Any: school / lab networks are often classified as "Public".
        New-NetFirewallRule -DisplayName $RuleName -Direction Inbound -Action Allow `
            -Protocol TCP -LocalPort $Ports -Profile Any | Out-Null
        Write-Host "Inbound TCP $($Ports -join ', ') allowed." -ForegroundColor Green
        Show-LanAddresses
    }
    'firewall-remove' {
        if (-not (Test-Admin)) { throw 'Run PowerShell as Administrator for this step.' }
        Get-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule
        Write-Host 'Firewall rule removed.' -ForegroundColor Green
    }
    'worker' {
        Set-CommonEnv
        $env:WORKER_GRPC_ADDR = '127.0.0.1:50052'   # internal only, never exposed
        if ($MaxActiveJobs -gt 0) { $env:MAX_ACTIVE_JOBS = "$MaxActiveJobs" }
        if ($QueueCapacity -gt 0) { $env:QUEUE_CAPACITY = "$QueueCapacity" }
        Write-Host "Starting worker-engine (MAX_ACTIVE_JOBS=$($env:MAX_ACTIVE_JOBS), QUEUE_CAPACITY=$($env:QUEUE_CAPACITY); empty = value from .env)" -ForegroundColor Cyan
        cargo run @cargoProfile -p worker-engine
    }
    'gateway' {
        Set-CommonEnv
        $env:GATEWAY_HTTP_ADDR = '0.0.0.0:8080'
        $env:GATEWAY_GRPC_ADDR = '0.0.0.0:50051'
        $env:WORKER_ENDPOINT = 'http://127.0.0.1:50052'
        Show-LanAddresses
        Show-FirewallState
        Write-Host 'Starting api-gateway on 0.0.0.0 (reachable from the LAN)...' -ForegroundColor Cyan
        cargo run @cargoProfile -p api-gateway
    }
}
