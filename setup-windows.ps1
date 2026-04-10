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
    Write-Host "  NFS Client: failed — $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "  This is optional. SMB shares work without it." -ForegroundColor Gray
}

# 2. Install ffmpeg
Write-Host ""
Write-Host "Installing ffmpeg..." -ForegroundColor Yellow
$ffmpegPath = Get-Command ffmpeg -ErrorAction SilentlyContinue
if ($ffmpegPath) {
    Write-Host "  ffmpeg: already installed at $($ffmpegPath.Source)" -ForegroundColor Green
} else {
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($winget) {
        Write-Host "  Installing via winget..."
        winget install --id Gyan.FFmpeg -e --accept-package-agreements --accept-source-agreements 2>&1 | Out-Null
        # Refresh PATH
        $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")
    } else {
        Write-Host "  Downloading ffmpeg..."
        $url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
        $zip = "$env:TEMP\ffmpeg.zip"
        $dest = "C:\ffmpeg"
        Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
        Expand-Archive -Path $zip -DestinationPath $dest -Force
        $bin = Get-ChildItem -Path $dest -Recurse -Filter "ffmpeg.exe" | Select-Object -First 1
        if ($bin) {
            $binDir = $bin.DirectoryName
            [Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";$binDir", "User")
            $env:PATH += ";$binDir"
            Write-Host "  ffmpeg installed to $binDir" -ForegroundColor Green
        }
        Remove-Item $zip -Force -ErrorAction SilentlyContinue
    }
    # Verify
    $ffmpegPath = Get-Command ffmpeg -ErrorAction SilentlyContinue
    if ($ffmpegPath) {
        Write-Host "  ffmpeg: OK" -ForegroundColor Green
    } else {
        Write-Host "  ffmpeg: installed but may need a new terminal to be in PATH" -ForegroundColor Yellow
    }
}

# 3. Check Python
Write-Host ""
Write-Host "Verification:" -ForegroundColor Cyan
$python = Get-Command python -ErrorAction SilentlyContinue
if ($python) {
    $ver = & python --version 2>&1
    Write-Host "  Python: $ver" -ForegroundColor Green
} else {
    Write-Host "  Python: NOT FOUND — install from https://python.org" -ForegroundColor Red
}

Write-Host ""
Write-Host "Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Run the agent:  python agent.py --server https://your-domain.com/imdb --user yourname"
Write-Host "  2. It will create agent.json — edit it with your server details"
Write-Host "  3. Run again to sync your library"
Write-Host ""
Write-Host "Press any key to close..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
