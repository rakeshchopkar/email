# author=Rakesh chopkar

from crontab import CronTab

cron = CronTab(user='rakesh')
job = cron.new(command='python /home/rakesh/email/mail_send.py >> /home/rakesh/email/cron.log 2>&1')
#job.minute.every(1)
job.every(1).days()
cron.write()