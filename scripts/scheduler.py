from crontab import CronTab
from datetime import date

cron = CronTab(user=True)

python3 = '/usr/bin/python3'
attractions_path = '/home/bdm/triphawk/scripts/collectors/attractions/foursquare.py'
businesses_path = '/home/bdm/triphawk/scripts/collectors/businesses/yelp.py'

#TODO fix the period
attractions = cron.new(command=f'{python3} {attractions_path}')
businesses = cron.new(command=f'{python3} {businesses_path}')
# attractions.minute.every(1)


cron.write()