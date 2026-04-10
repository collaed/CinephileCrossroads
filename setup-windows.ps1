# Self-elevate if not running as admin
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Requesting administrator privileges..." -ForegroundColor Yellow
    Start-Process powershell.exe "-ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

Write-Host "CineCross Windows Setup" -ForegroundColor Cyan
Write-Host ""

# 1. Install NFS Client
Write-Host "Installing NFS Client..." -ForegroundColor Yellow
try {
    $nfs = Get-WindowsOptionalFeature -Online -FeatureName "ServicesForNFS-ClientOnly" -ErrorAction Stop
    if ($nfs.State -eq "Enabled") {
        Write-Host "  NFS Client: already installed" -ForegroundColor Green
    } else {
        Enable-WindowsOptionalFeature -Online -FeatureName "ServicesForNFS-ClientOnly" -All -NoRestart -ErrorAction Stop
        Write-Host "  NFS Client: installed (reboot may be needed)" -ForegroundColor Green
    }
} catch {
    Write-Host "  NFS Client: skipped (optional, SMB works without it)" -ForegroundColor Gray
}

# 2. Install ffmpeg
Write-Host ""
Write-Host "Installing ffmpeg..." -ForegroundColor Yellow
$ffmpegPath = Get-Command ffmpeg -ErrorAction SilentlyContinue
if ($ffmpegPath) {
    Write-Host "  ffmpeg: already installed" -ForegroundColor Green
} else {
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($winget) {
        Write-Host "  Installing via winget..."
        winget install --id Gyan.FFmpeg -e --accept-package-agreements --accept-source-agreements
    } else {
        Write-Host "  Downloading ffmpeg..."
        $url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
        $zip = Join-Path $env:TEMP "ffmpeg.zip"
        $dest = "C:\ffmpeg"
        Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
        Expand-Archive -Path $zip -DestinationPath $dest -Force
        $bin = Get-ChildItem -Path $dest -Recurse -Filter "ffmpeg.exe" | Select-Object -First 1
        if ($bin) {
            $binDir = $bin.DirectoryName
            $userPath = [Environment]::GetEnvironmentVariable("PATH", "User")
            [Environment]::SetEnvironmentVariable("PATH", "$userPath;$binDir", "User")
            $env:PATH = "$env:PATH;$binDir"
            Write-Host "  ffmpeg installed to $binDir" -ForegroundColor Green
        }
        Remove-Item $zip -Force -ErrorAction SilentlyContinue
    }
}

# 3. Verify
Write-Host ""
Write-Host "Verification:" -ForegroundColor Cyan

$python = Get-Command python -ErrorAction SilentlyContinue
if ($python) { Write-Host "  Python: OK" -ForegroundColor Green }
else { Write-Host "  Python: NOT FOUND — install from https://python.org" -ForegroundColor Red }

$ff = Get-Command ffmpeg -ErrorAction SilentlyContinue
if ($ff) { Write-Host "  ffmpeg: OK" -ForegroundColor Green }
else { Write-Host "  ffmpeg: not in PATH yet (open a new terminal)" -ForegroundColor Yellow }

Write-Host ""
Write-Host "Done! Press any key to close..." -ForegroundColor Green
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
