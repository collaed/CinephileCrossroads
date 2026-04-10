# Self-elevate if not running as admin
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Requesting administrator privileges..." -ForegroundColor Yellow
    Start-Process powershell.exe "-ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

Write-Host "CineCross Windows Setup" -ForegroundColor Cyan
Write-Host ""

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
        $ffUrl = 'https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip'
        $ffZip = Join-Path $env:TEMP 'ffmpeg.zip'
        $ffDest = Join-Path $env:ProgramFiles 'ffmpeg'
        Invoke-WebRequest -Uri $ffUrl -OutFile $ffZip -UseBasicParsing
        Expand-Archive -Path $ffZip -DestinationPath $ffDest -Force
        $ffBin = Get-ChildItem -Path $ffDest -Recurse -Filter 'ffmpeg.exe' | Select-Object -First 1
        if ($ffBin) {
            $ffBinDir = $ffBin.DirectoryName
            $currentPath = [Environment]::GetEnvironmentVariable('PATH', 'User')
            [Environment]::SetEnvironmentVariable('PATH', ($currentPath + ';' + $ffBinDir), 'User')
            $env:PATH = $env:PATH + ';' + $ffBinDir
            Write-Host ('  ffmpeg installed to ' + $ffBinDir) -ForegroundColor Green
        }
        Remove-Item $ffZip -Force -ErrorAction SilentlyContinue
    }
}

# 3. Verify
Write-Host ""
Write-Host "Verification:" -ForegroundColor Cyan

$python = Get-Command python -ErrorAction SilentlyContinue
if ($python) { Write-Host "  Python: OK" -ForegroundColor Green }
else { Write-Host "  Python: NOT FOUND - install from https://python.org" -ForegroundColor Red }

$ff = Get-Command ffmpeg -ErrorAction SilentlyContinue
if ($ff) { Write-Host "  ffmpeg: OK" -ForegroundColor Green }
else { Write-Host "  ffmpeg: not in PATH yet (open a new terminal)" -ForegroundColor Yellow }

Write-Host ""
Write-Host "Done! Press any key to close..." -ForegroundColor Green
$null = $Host.UI.RawUI.ReadKey('NoEcho,IncludeKeyDown')
