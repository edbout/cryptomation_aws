#!/bin/bash
cd /home/ec2-user/cryptomation_aws
git pull origin main
# bundle install
# source ./venv/bin/activate
# pip install
sudo systemctl restart dashboard
sudo systemctl restart cryptomation
echo "Main bot updated and restarted."
