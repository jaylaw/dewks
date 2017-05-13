
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np


# In[2]:

from flask import Flask
from flask_sqlalchemy import SQLAlchemy


# In[3]:

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, DateTime, Float, ForeignKey, Integer, SmallInteger, String, Text, UniqueConstraint
from sqlalchemy.orm import relationship


# In[4]:

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://postgres@localhost/dev_logware3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
conn = db.engine.connect().connection


# In[5]:

class Annotation(db.Model):
    __tablename__ = 'annotations'

    annotation_guid = db.Column(db.String(32), primary_key=True)
    reading_guid = db.Column(db.ForeignKey('readings.reading_guid'), nullable=False)
    annotation = db.Column(db.Text)

    reading = db.relationship('Reading', primaryjoin='Annotation.reading_guid == Reading.reading_guid', backref='annotations')


class Asset(db.Model):
    __tablename__ = 'assets'
    __table_args__ = (
        db.CheckConstraint("(model)::text <> ''::text"),
        db.CheckConstraint("(serial)::text <> ''::text"),
        db.UniqueConstraint('model', 'serial')
    )

    asset_guid = db.Column(db.String(32), primary_key=True)
    asset_type = db.Column(db.SmallInteger, nullable=False)
    model = db.Column(db.String(32), nullable=False)
    serial = db.Column(db.String(32), nullable=False)
    active = db.Column(db.Boolean)
    deleted = db.Column(db.Boolean)
    asset_password = db.Column(db.String(20))
    notes = db.Column(db.Text)


class LicenseInUse(db.Model):
    __tablename__ = 'license_in_use'

    license_in_use_guid = db.Column(db.String(32), primary_key=True)
    computer_name = db.Column(db.Text, nullable=False)
    user_guid = db.Column(db.ForeignKey('users.user_guid'), nullable=False)
    license_guid = db.Column(db.ForeignKey('licenses.license_guid'), nullable=False)
    time_stamp = db.Column(db.DateTime, nullable=False)

    license = db.relationship('License', primaryjoin='LicenseInUse.license_guid == License.license_guid', backref='license_in_uses')
    user = db.relationship('User', primaryjoin='LicenseInUse.user_guid == User.user_guid', backref='license_in_uses')


class License(db.Model):
    __tablename__ = 'licenses'

    license_guid = db.Column(db.String(32), primary_key=True)
    license_type = db.Column(db.SmallInteger, nullable=False)
    license_serial = db.Column(db.String(20))
    version = db.Column(db.String(20))
    date_applied = db.Column(db.DateTime, nullable=False)
    logins_remaining = db.Column(db.Integer)
    license_id = db.Column(db.Text, nullable=False)
    deleted = db.Column(db.Boolean)


class Location(db.Model):
    __tablename__ = 'locations'
    __table_args__ = (
        db.CheckConstraint("(location_name)::text <> ''::text"),
    )

    location_guid = db.Column(db.String(32), primary_key=True)
    location_name = db.Column(db.String(20), nullable=False, unique=True)
    active = db.Column(db.Boolean)
    deleted = db.Column(db.Boolean)
    notes = db.Column(db.Text)


class LogSession(db.Model):
    __tablename__ = 'log_sessions'

    log_session_guid = db.Column(db.String(32), primary_key=True)
    session_start = db.Column(db.DateTime, nullable=False)
    session_end = db.Column(db.DateTime)
    logging_interval = db.Column(db.Integer, nullable=False)
    logger_guid = db.Column(db.ForeignKey('assets.asset_guid'), nullable=False)
    user_guid = db.Column(db.ForeignKey('users.user_guid'), nullable=False)
    session_type = db.Column(db.SmallInteger, nullable=False)
    computer_name = db.Column(db.Text, nullable=False)

    asset = db.relationship('Asset', primaryjoin='LogSession.logger_guid == Asset.asset_guid', backref='log_sessions')
    user = db.relationship('User', primaryjoin='LogSession.user_guid == User.user_guid', backref='log_sessions')


class Reading(db.Model):
    __tablename__ = 'readings'

    reading_guid = db.Column(db.String(32), primary_key=True)
    reading = db.Column(db.Float(53), nullable=False)
    reading_type = db.Column(db.SmallInteger, nullable=False)
    time_stamp = db.Column(db.DateTime, nullable=False)
    log_session_guid = db.Column(db.ForeignKey('log_sessions.log_session_guid'), nullable=False)
    sensor_guid = db.Column(db.ForeignKey('assets.asset_guid'), nullable=False)
    location_guid = db.Column(db.ForeignKey('locations.location_guid'), nullable=False)
    channel = db.Column(db.SmallInteger, nullable=False)
    max_alarm = db.Column(db.Boolean)
    max_alarm_value = db.Column(db.Float(53))
    min_alarm = db.Column(db.Boolean)
    min_alarm_value = db.Column(db.Float(53))
    compromised = db.Column(db.Boolean)

    location = db.relationship('Location', primaryjoin='Reading.location_guid == Location.location_guid', backref='readings')
    log_session = db.relationship('LogSession', primaryjoin='Reading.log_session_guid == LogSession.log_session_guid', backref='readings')
    asset = db.relationship('Asset', primaryjoin='Reading.sensor_guid == Asset.asset_guid', backref='readings')


class SensorParameter(db.Model):
    __tablename__ = 'sensor_parameters'

    log_session_guid = db.Column(db.ForeignKey('log_sessions.log_session_guid'), primary_key=True, nullable=False)
    channel = db.Column(db.SmallInteger, primary_key=True, nullable=False)
    parameter_name = db.Column(db.String(128), primary_key=True, nullable=False)
    parameter_value = db.Column(db.String(128), nullable=False)

    log_session = db.relationship('LogSession', primaryjoin='SensorParameter.log_session_guid == LogSession.log_session_guid', backref='sensor_parameters')


class User(db.Model):
    __tablename__ = 'users'
    __table_args__ = (
        db.CheckConstraint("(login_name)::text <> ''::text"),
    )

    user_guid = db.Column(db.String(32), primary_key=True)
    login_name = db.Column(db.String(32), nullable=False, unique=True)
    first_name = db.Column(db.String(64))
    last_name = db.Column(db.String(64))
    user_password = db.Column(db.String(64))
    user_group = db.Column(db.SmallInteger)
    permissions = db.Column(db.BigInteger)
    active = db.Column(db.Boolean)
    deleted = db.Column(db.Boolean)
    change = db.Column(db.Boolean)
    notes = db.Column(db.Text)


class Version(db.Model):
    __tablename__ = 'versions'

    db_version = db.Column(db.String(20), primary_key=True)
    client_version = db.Column(db.String(20))


# In[6]:

records = db.session.query(Reading).join(Reading.location).filter_by(location_name='ONSITE1').filter(Reading.time_stamp.between('2017-03-26', '2017-03-28'))


# In[143]:

criteria = {"location_name_1": 'ONSITE1', "time_stamp_1":'2017-01-26', "time_stamp_2":'2017-03-28'}


# In[144]:

df = pd.read_sql(('''
    SELECT readings.reading_guid, readings.reading , readings.reading_type , readings.time_stamp , locations.location_name , readings.sensor_guid , readings.location_guid AS readings_location_guid, readings.channel AS readings_channel, readings.max_alarm AS readings_max_alarm, readings.max_alarm_value AS readings_max_alarm_value, readings.min_alarm AS readings_min_alarm, readings.min_alarm_value AS readings_min_alarm_value, readings.compromised AS readings_compromised 
FROM readings JOIN locations ON readings.location_guid = locations.location_guid 
WHERE locations.location_name = %(location_name_1)s AND readings.time_stamp BETWEEN %(time_stamp_1)s AND %(time_stamp_2)s'''),
                conn, params=criteria)
df.info()


# In[145]:

df


# In[146]:

def celsius_to_fahr(temp_celsius):
    """Convert Fahrenheit to Celsius
    
    Return Celsius conversion of input"""
    temp_fahr = (temp_celsius * 1.8) + 32
    return temp_fahr


# In[147]:

# convert temps to fahrenheit
df.loc[df['reading_type'] == 0, 'reading'] = df.reading.apply(celsius_to_fahr)
df


# In[148]:

locs = [loc for loc, in db.session.query(Location.location_name)]


# In[149]:

summary = pd.DataFrame(index=None, columns=['LOCATION', 'SPECIFICATION', 'START_DATE', 'END_DATE', 'FIRST_POINT_RECORDED', 'LAST_POINT_RECORDED', 'TOTAL_HOURS_EVALUATED', 'TOTAL_HOURS_RECORDED', 'TOTAL_HOURS_OUT', 'PERCENT_OUT', 'HOURS_TEMP_HIGH', 'HOURS_TEMP_LOW', 'HOURS_RH_HIGH', 'HOURS_RH_LOW', 'HOURS_OVERLAP', 'HOURS_NO_DATA', 'INT_GREATER_THAN_15', 'HRS_DOWN_FOR_MAINT', 'DUPE_RECORDS'])
summary


# In[150]:

df.location_name.unique()


# In[151]:

summary.LOCATION = df.location_name.unique()
summary


# In[153]:

temps = df[df['reading_type']==0]
temps.dtypes


# In[154]:

temps = temps.set_index('time_stamp')


# In[205]:

temps['duration'] = temps.index.to_series().diff().dt.seconds.div(60, fill_value=0)
temps.describe()


# In[202]:

temp_hi = temps[temps.reading > 72]
temp_low = temps[temps.reading < 69]
temp_low.describe()


# In[196]:

t_hr_hi = temp_hi.duration.sum(axis=0) / 60
t_hr_hi


# In[204]:

t_hr_low = temp_low.duration.sum(axis=0) / 60
t_hr_low


# In[238]:

t_gap = temps[temps.duration > 15]
t_gap_hrs = t_gap.duration.sum(axis=0) / 60
t_gap


# In[239]:

t_gap_hrs


# In[247]:




# In[226]:

rh = df[df['reading_type']==1]
rh.dtypes


# In[227]:

rh = rh.set_index('time_stamp')


# In[228]:

rh['duration'] = rh.index.to_series().diff().dt.seconds.div(60, fill_value=0)
rh.describe()


# In[229]:

rh_hi = rh[rh.reading > 29.5]
rh_low = rh[rh.reading < 25.5]
rh_low.describe()


# In[230]:

rh_hr_hi = rh_hi.duration.sum(axis=0) / 60
rh_hr_hi


# In[231]:

rh_hr_low = rh_low.duration.sum(axis=0) / 60
rh_hr_low


# In[264]:

rh_gap = rh[rh.duration > 15]
rh_gap_hrs = rh_gap.duration.sum(axis=0)
rh_gap


# In[237]:

rh_gap_hrs


# In[255]:


# In[252]:



# In[269]:

pd.concat([t_gap, rh_gap], keys=['reading_type', 'time_stamp'])


# In[271]:

len(pd.merge(t_gap, rh_gap, left_index=True, right_index=True))


# In[ ]:




# In[281]:

# df.loc[df['line_race'] == 0, 'rating'] = 0
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'START_DATE'] = pd.to_datetime(criteria.get('time_stamp_1'))
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'END_DATE'] = pd.to_datetime(criteria.get('time_stamp_2'))
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'FIRST_POINT_RECORDED'] = df.time_stamp.min()
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'LAST_POINT_RECORDED'] = df.time_stamp.max()
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'TOTAL_HOURS_EVALUATED'] = (summary.END_DATE - summary.START_DATE).astype('timedelta64[s]') / 3600.0
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'HOURS_TEMP_HIGH'] = temp_hi.duration.sum(axis=0) / 60
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'HOURS_TEMP_LOW'] = temp_low.duration.sum(axis=0) / 60
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'TOTAL_HOURS_RECORDED'] = ((summary.LAST_POINT_RECORDED - summary.FIRST_POINT_RECORDED).astype('timedelta64[s]') / 3600.0) - t_gap_hrs
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'HOURS_NO_DATA'] = t_gap_hrs
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'INT_GREATER_THAN_15'] = len(pd.merge(t_gap, rh_gap, left_index=True, right_index=True))
summary.loc[summary.LOCATION == criteria.get('location_name_1'), 'TOTAL_HOURS_OUT'] = summary[['HOURS_TEMP_HIGH', 'HOURS_TEMP_LOW', 'HOURS_RH_HIGH', 'HOURS_RH_LOW']].sum(axis=1)

summary


# In[282]:

criteria.get('location_name_1')


# In[ ]:



