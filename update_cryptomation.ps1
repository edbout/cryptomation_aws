# update_cryptomation.ps1
# Run this on your Windows machine to execute the update script on the EC2 instance.

$ScriptDir    = Split-Path -Parent $MyInvocation.MyCommand.Path
$PemFile      = Join-Path $ScriptDir "ireland.pem"
$RemoteHost   = "ec2-user@ec2-54-229-189-249.eu-west-1.compute.amazonaws.com"
$RemoteCommand = 'cd cryptomation_aws && sh ./update_cryptomation.sh'

Write-Host "Executing update on EC2..."
ssh -i "$PemFile" -o StrictHostKeyChecking=no "$RemoteHost" "$RemoteCommand"

if ($LASTEXITCODE -eq 0) {
    Write-Host "Done. Update script executed successfully."
} else {
    Write-Host "SSH command failed (exit code $LASTEXITCODE). Check VPN / security group / key permissions / script existence."
    exit 1
}