# Self-elevate if not running as admin
if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Requesting administrator privileges..." -ForegroundColor Yellow
    Start-Process powershell.exe "-ExecutionPolicy Bypass -File `"$PSCommandPath`"" -Verb RunAs
    exit
}

# CineCross Windows Setup — installs ffmpeg and NFS client
# Run as Administrator: powershell -ExecutionPolicy Bypass -File setup-windows.ps1

Write-Host "CineCross Windows Setup" -ForegroundColor Cyan
Write-Host ""

# 1. Install NFS Client (Windows feature)
Write-Host "Installing NFS Client..." -ForegroundColor Yellow
$nfs = Get-WindowsOptionalFeature -Online -FeatureName "ServicesForNFS-ClientOnly" -ErrorAction SilentlyContinue
if ($nfs.State -eq "Enabled") {
    Write-Host "  NFS Client already installed" -ForegroundColor Green
} else {
    Enable-WindowsOptionalFeature -Online -FeatureName "ServicesForNFS-ClientOnly" -All -NoRestart
    Write-Host "  NFS Client installed (reboot may be needed)" -ForegroundColor Green
}

# 2. Install ffmpeg via winget or direct download
Write-Host ""
Write-Host "Installing ffmpeg..." -ForegroundColor Yellow
$ffmpeg = Get-Command ffmpeg -ErrorAction SilentlyContinue
if ($ffmpeg) {
    Write-Host "  ffmpeg already installed: $($ffmpeg.Source)" -ForegroundColor Green
} else {
    # Try winget first
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if ($winget) {
        Write-Host "  Installing via winget..."
        winget install --id Gyan.FFmpeg -e --accept-package-agreements --accept-source-agreements
    } else {
        # Direct download
        $url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
        $zip = "$env:TEMP\ffmpeg.zip"
        $dest = "C:\ffmpeg"
        Write-Host "  Downloading from $url..."
        Invoke-WebRequest -Uri $url -OutFile $zip
        Expand-Archive -Path $zip -DestinationPath $dest -Force
        # Find the bin folder and add to PATH
        $bin = Get-ChildItem -Path $dest -Recurse -Filter "ffmpeg.exe" | Select-Object -First 1
        if ($bin) {
            $binDir = $bin.DirectoryName
            $env:PATH += ";$binDir"
            [Environment]::SetEnvironmentVariable("PATH", $env:PATH, "User")
            Write-Host "  ffmpeg installed to $binDir and added to PATH" -ForegroundColor Green
        }
        Remove-Item $zip -Force
    }
}

# 3. Verify
Write-Host ""
Write-Host "Verification:" -ForegroundColor Cyan
$checks = @(
    @{Name="Python"; Cmd="python --version"},
    @{Name="ffmpeg"; Cmd="ffmpeg -version"},
    @{Name="NFS"; Cmd="mount"}
)
foreach ($c in $checks) {
    try {
        $out = Invoke-Expression $c.Cmd 2>&1 | Select-Object -First 1
        Write-Host "  $($c.Name): $out" -ForegroundColor Green
    } catch {
        Write-Host "  $($c.Name): NOT FOUND" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Setup complete. You can now:" -ForegroundColor Cyan
Write-Host "  1. Mount NFS shares:  mount \\zeus\Movies Z:" -ForegroundColor White
Write-Host "  2. Run the agent:     python agent.py --server URL --user yourname" -ForegroundColor White
Write-Host "  3. Edit agent.json:   set _path_mappings to map Kodi paths to local paths" -ForegroundColor White
