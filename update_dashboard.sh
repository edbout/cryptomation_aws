#!/bin/bash
cd /home/ec2-user/cryptomation_aws
git pull origin main
bundle install
sudo systemctl restart dashboard
echo "Dashboard updated and restarted."