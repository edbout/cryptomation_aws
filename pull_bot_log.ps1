# pull_bot_log.ps1
# Run this on your Windows machine before (or instead of) the scheduled Claude analysis.
# It pulls the latest bot.log from the EC2 instance into the local log/ folder.

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$PemFile    = Join-Path $ScriptDir "ireland.pem"
$RemoteHost = "ec2-user@ec2-54-229-189-249.eu-west-1.compute.amazonaws.com"
$RemotePath = "./cryptomation_aws/log/bot.log"
$LocalPath  = Join-Path $ScriptDir "log\bot.log"

Write-Host "Pulling bot.log from EC2..."
scp -i "$PemFile" -o StrictHostKeyChecking=no "${RemoteHost}:${RemotePath}" "$LocalPath"

if ($LASTEXITCODE -eq 0) {
    Write-Host "Done. Saved to: $LocalPath"
} else {
    Write-Host "SCP failed (exit code $LASTEXITCODE). Check VPN / security group / key permissions."
    exit 1
}
