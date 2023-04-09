# The solution to this problem involves three main steps

# 1. Ingest the CSV files into a database
# 2. Compute the uptime and downtime for each store in the last hour, day and week
# 3. Generate a report in CSV format with the computed values


##  Step 1: Ingest the CSV files into a database
import os
import threading
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

# create the engine
engine = create_engine('postgresql://postgres:shafa@localhost:5432/postgres', echo=True)

# create the session
Session = sessionmaker(bind=engine)
session = Session()

# create the base class
Base = declarative_base()

# define the store model
class Store(Base):
    __tablename__ = 'stores'

    store_id = Column(String, primary_key=True)
    status = Column(String)
    timestamp_utc = Column(DateTime)

# define the store hours model
class StoreHours(Base):
    __tablename__ = 'store_hours'

    id = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(String)
    day = Column(Integer)
    start_time_local = Column(String)
    end_time_local = Column(String)

# define the timezone model
class Timezone(Base):
    __tablename__ = 'timezones'

    store_id = Column(String, primary_key=True)
    timezone_str = Column(String)

# create the tables
Base.metadata.create_all(engine)

# read the CSV files
stores_df = pd.read_csv('store_status.csv')
hours_df = pd.read_csv('store_hours.csv')
timezones_df = pd.read_csv('stores.csv')

# write to the database
stores_df.to_sql('stores', engine, if_exists='replace', index=False)
hours_df.to_sql('store_hours', engine, if_exists='replace', index=False)
timezones_df.to_sql('timezones', engine, if_exists='replace', index=False)

# commit the changes
session.commit()

## Step 2: Compute the uptime and downtime for each store

from datetime import datetime, timedelta
import pytz
import pandas as pd

# function to compute the uptime and downtime for a store
def compute_uptime_downtime(store_id, start_time, end_time):
    # get the timezone for the store
    timezone = session.query(Timezone).filter_by(store_id=store_id).first().timezone_str
    
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    # convert the start and end time to UTC
    start_time_utc = pytz.timezone(timezone).localize(start_time).astimezone(pytz.utc)
    end_time_utc = pytz.timezone(timezone).localize(end_time).astimezone(pytz.utc)
    
    # get the store hours for the day of the start time
    day_of_week = start_time_utc.weekday()
    store_hours = session.query(StoreHours).filter_by(store_id=store_id, day=day_of_week).all()
    
    # if there are no store hours for the day, assume it is open 24*7
    if not store_hours:
        store_hours = [(None, start_time_utc.replace(hour=0, minute=0, second=0, microsecond=0), end_time_utc.replace(hour=23, minute=59, second=59, microsecond=0))]
    
    # create a list of time intervals for the store hours
    intervals = []
    for store_hour in store_hours:
        start_time_local = datetime.strptime(store_hour.start_time_local, '%H:%M:%S').time()
        end_time_local = datetime.strptime(store_hour.end_time_local, '%H:%M:%S').time()
        start_time = datetime.combine(start_time_utc.date(), start_time_local)
        end_time = datetime.combine(end_time_utc.date(), end_time_local)
        if end_time < start_time:
            end_time += timedelta(days=1)
        intervals.append((start_time, end_time))
    
    # initialize the uptime and downtime to zero
    uptime = timedelta(0)
    downtime = timedelta(0)
    
    # iterate over the time intervals and compute the uptime and downtime
    for interval in intervals:
        # get the observations for the interval
        observations = session.query(Store).filter_by(store_id=store_id).filter(Store.timestamp_utc >= interval[0]).filter(Store.timestamp_utc < interval[1]).all()
        
        # compute the uptime and downtime for the interval
        interval_length = (interval[1] - interval[0]).total_seconds() / 60
        active_time = timedelta(0)
        inactive_time = timedelta(0)
        last_status = None
        for observation in observations:
            if observation.status == 'active':
                if last_status == 'inactive':
                    inactive_time += observation.timestamp_utc - last_timestamp
                active_time += observation.timestamp_utc - last_timestamp
                last_status = 'active'
            else:
                if last_status == 'active':
                    active_time += observation.timestamp_utc - last_timestamp
                inactive_time += observation.timestamp_utc - last_timestamp
                last_status = 'inactive'
            last_timestamp = observation.timestamp_utc
        if last_status == 'active':
            active_time += interval[1] - last_timestamp
        else:
            inactive_time += interval[1] - last_timestamp
        uptime += active_time
        downtime += inactive_time
    
    # convert the uptime and downtime to minutes and hours
    uptime_minutes = uptime.total_seconds() / 60
    downtime_minutes = downtime.total_seconds() / 60
    uptime_hours = uptime.total_seconds() / 3600
    downtime_hours = downtime.total_seconds() / 3600
    
    return uptime_minutes, uptime_hours, downtime_minutes, downtime_hours

# function to compute the uptime and downtime for all stores
def compute_uptime_downtime_all(start_time, end_time):
    # get the list of store ids
    store_ids = [store.store_id for store in session.query(Store).distinct(Store.store_id)]
    
    # initialize the report data
    report_data = []
    
    # iterate over the store ids and compute the uptime and downtime
    for store_id in store_ids:
        uptime_minutes, uptime_hours, downtime_minutes, downtime_hours = compute_uptime_downtime(store_id, start_time, end_time)
        report_data.append((store_id, uptime_minutes, uptime_hours, downtime_minutes, downtime_hours))
    
    # return the report data
    return report_data

## Step 3: Generate a report in CSV format

from flask import Flask, jsonify, request, send_file
import string
import random
import pandas as pd

# create the app
app = Flask(__name__)

# generate a random report id
def generate_report_id():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

# compute the report
def compute_report(start_time, end_time):
    # compute the uptime and downtime for all stores
    report_data = compute_uptime_downtime_all(start_time, end_time)
    
    # create a DataFrame from the report data
    report_df = pd.DataFrame(report_data, columns=['store_id', 'uptime_last_hour(in minutes)', 'uptime_last_hour(in hours)', 'downtime_last_hour(in minutes)', 'downtime_last_hour(in hours)'])
    print("Generated report:")
    print(report_df.head(5))
    
    # return the DataFrame as a CSV file
    return report_df.to_csv(index=False)

# trigger report endpoint
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # get the start and end time from the request body
    rx = request.get_json()
    start_time = rx['start_time'] if 'start_time' in rx else datetime.utcnow() - timedelta(hours=1) # format: YYYY-MM-DD HH:MM:SS
    end_time = rx['end_time'] if 'end_time' in rx else datetime.utcnow() # format: YYYY-MM-DD HH:MM:SS
    
    # generate a report id
    report_id = generate_report_id()
    
    # compute the report in the background
    def compute_report_background(report_id, start_time, end_time):
        report_csv = compute_report(start_time, end_time)
        with open(f'{report_id}.csv', 'w') as f:
            f.write(report_csv)
    thread = threading.Thread(target=compute_report_background, args=(report_id, start_time, end_time))
    thread.start()
    
    # return the report id
    return jsonify({'report_id': report_id})

# get report endpoint
@app.route('/get_report', methods=['GET'])
def get_report():
    # get the report id from the request parameters
    report_id = request.args.get('report_id')
    
    # check if the report CSV file exists
    if os.path.exists(f'{report_id}.csv'):
        # send the report CSV file
        return send_file(f'{report_id}.csv', mimetype='text/csv', as_attachment=True, attachment_filename=f'{report_id}.csv')
    else:
        # check if the report is still running
        if threading.active_count() > 1:
            return jsonify({'status': 'Running'})
        else:
            return jsonify({'status': 'Complete'})

# run the app
if __name__ == '__main__':
    app.run(debug=True, port=5000)

