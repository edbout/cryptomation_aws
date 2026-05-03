ssh -i "ireland.pem" ec2-user@ec2-54-229-189-249.eu-west-1.compute.amazonaws.com

NAME="Amazon Linux"
VERSION="2023"
ID="amzn"
ID_LIKE="fedora"
VERSION_ID="2023"
PLATFORM_ID="platform:al2023"
PRETTY_NAME="Amazon Linux 2023.11.20260427"
ANSI_COLOR="0;33"
CPE_NAME="cpe:2.3:o:amazon:amazon_linux:2023"
HOME_URL="https://aws.amazon.com/linux/amazon-linux-2023/"
DOCUMENTATION_URL="https://docs.aws.amazon.com/linux/"
SUPPORT_URL="https://aws.amazon.com/premiumsupport/"
BUG_REPORT_URL="https://github.com/amazonlinux/amazon-linux-2023"
VENDOR_NAME="AWS"

# Create a new SSH keypair for this machine
ssh-keygen -t ed25519 -C "aws-ec2-polymarket-bot" -f ~/.ssh/id_ec2_git

The key's randomart image is:
+--[ED25519 256]--+
|oo.. .    ...    |
|=.. o    o .     |
|oo .      * .    |
|o o      + =     |
|.o .  . S = .    |
|.E.  + o B   .   |
|   ++ * o o   .  |
| .+..* = o + .   |
|o+.o+.+ . o .    |
+----[SHA256]-----+

ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFWhD78A9+4VyS2f9ls8qlcUJQUPRbA6vCwkepSkG7ps aws-ec2-polymarket-bot

sudo dnf install -y git

nano ~/.ssh/config

Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ec2_git
  IdentitiesOnly yes

sudo yum update -y
sudo yum install -y python3 python3-pip git tmux curl htop jq

mkdir ~/cryptomation-bot

cd ~/cryptomation-aws
git pull origin main

source venv/bin/activate
pip install --upgrade pip

pip install -r requirements.txt