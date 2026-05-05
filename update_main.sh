#!/bin/bash
cd /home/ec2-user/cryptomation_aws
git pull origin main
sudo systemctl restart cryptomation
echo "Main bot updated and restarted."
