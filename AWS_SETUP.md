ssh -i "ireland.pem" ec2-user@ec2-54-229-189-249.eu-west-1.compute.amazonaws.com
source ./venv/bin/activate

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


Keep Redis Cloud. Here's the reasoning:

Latency doesn't matter here. The trading bot's timing is gated by 5-minute candle windows and 5-second position management intervals. Your Redis calls are all state management (signal recording, order tracking, stats caching) — none of them sit in the critical path between "signal fires" and "order hits the exchange." That path is: Bybit WebSocket tick → _on_ticker → execute_trading_validation → Polymarket CLOB API. Redis is not in that chain. A 1–3ms round-trip to Redis Cloud versus <0.1ms local makes zero measurable difference.

The real risk of going local is data loss. You've accumulated 13K+ prices:fairvalue records per asset — that's weeks of fair value reference data the bot depends on for edge calculation. A local Redis with default config (save 3600 1) loses up to an hour of writes on a crash or EC2 reboot. With Redis Cloud, persistence is managed for you. To do local safely you'd need AOF persistence (appendonly yes) configured and tested, plus a systemd service that restarts Redis before the bot. That's maintenance burden with no trading benefit.

The only reason to switch to local would be if you're paying meaningfully for Redis Cloud and the free/cheap tier doesn't cover your data volume. At your scale — 5 assets, ~10 fairvalue samples per 5-minute candle, signal records — you're using well under 50MB. That fits in Redis Cloud's free tier indefinitely.

Short answer: same-region Redis Cloud is the right call. Spend your time on the bot logic, not Redis ops.

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
rm -rf venv
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip

cd ~/polymarket-bot
source venv/bin/activate
pip install -r requirements.txt

sudo dnf update -y
sudo dnf install -y redis6

sudo systemctl enable redis6
sudo systemctl start redis6
sudo systemctl status redis6


sudo dnf swap curl-minimal curl-full
sudo dnf swap libcurl-minimal libcurl-full

for i in {1..100}; do
  curl -s -o /dev/null -w "%{time_total}\n" \
    -H "User-Agent: latency-test" \
    https://clob.polymarket.com
  sleep 0.05
done > latencies.log


Redis localy
StrongPass


Install it if needed: sudo dnf install tmux

Start a session: tmux new -s bot

Run your script inside: python main.py

Detach: Press Ctrl+b then d.

You can now safely disconnect. To check on it later, log back in and run: tmux attach -t bot