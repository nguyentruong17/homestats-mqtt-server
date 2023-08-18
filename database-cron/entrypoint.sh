#!/bin/sh

# Start the run once job.
echo "Docker container has been started"

# Setup a cron schedule
echo "* * * * * /usr/local/bin/python3 /usr/src/database-cron/publish-remote.py >> /var/log/cron-publish-remote.log 2>&1
# This extra line makes it a valid cron" > scheduler.txt

crontab scheduler.txt
crond -f -l 2
