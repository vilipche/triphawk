from crontab import CronTab
from datetime import date

cron = CronTab(user=True)

python3 = '/usr/bin/python3'
ATTRACTIONS_PATH = '/home/bdm/triphawk/scripts/pipelines/attractions.py'
BUSINESSES_PATH = '/home/bdm/triphawk/scripts/pipelines/businesses.py'
ACCOMODATIONS_PATH = '/home/bdm/triphawk/scripts/pipelines/accomodation.py'

attractions = cron.new(command=f'{python3} {ATTRACTIONS_PATH}', comment="Run the attractions pipeline")
businesses = cron.new(command=f'{python3} {BUSINESSES_PATH}', comment="Run the businesses pipeline")
accomodations = cron.new(command=f'{python3} {ACCOMODATIONS_PATH}',comment="Run the accomodations pipeline")


attractions.month.every(1) # run every month
businesses.day.on(1) #run every MON
accomodations.day.on(2) # run every TUE

cron.write()