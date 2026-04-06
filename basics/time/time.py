import datetime
import time
import pytz

def main():
    current_time = datetime.datetime.now()
    print(current_time.strftime("%Y-%m-%d"))
    timestamp = time.time()
    print(timestamp)
    utc_time = datetime.datetime.utcfromtimestamp(timestamp)
    print("utc_time",utc_time)
    target_timezone = pytz.timezone('Asia/Shanghai')
    target_time = utc_time.replace(tzinfo=pytz.timezone).astimezone(target_timezone)
    print(target_time)
    pass

if __name__ == '__main__':
    main()