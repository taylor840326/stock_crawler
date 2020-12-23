#!/usr/bin/env python
# -*- coding: utf8 -*-

from crontab import CronTab

#用python操作crontab，写入定时任务。
if __name__ == "__main__":
    cron = CronTab(user='taylor')
    job = cron.new(command='echo HellWorld', comment='backup2')
    job.minute.on(1)
    cron.write()
