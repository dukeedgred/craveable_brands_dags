from datetime import datetime, timedelta, timezone, date
import pytz

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

sydney_tz = pytz.timezone('Australia/Sydney')
current_date_time = datetime.now(sydney_tz)#.strftime("%Y-%m-%d")
previous_date_time = current_date_time + timedelta(days=-1) #+ datetime.timedelta(days=1)
previous_date_time_7_days = current_date_time + timedelta(days=-7) #+ datetime.timedelta(days=1)
previous_date = previous_date_time.strftime("%Y-%m-%d")

for single_date in daterange(previous_date_time, current_date_time):
    print(single_date)