# Main code to get data from Higeco server using its API with some preprocessing functions

import requests
import json
from datetime import datetime
from datetime import timedelta
import pytz
import time
import csv
import pandas as pd

# Get Credentials (using config module)
params = config()
host = params['host']
username = params['username']
password = params['password']
apiToken = params['apitoken']

# Connection session
session = requests.Session()

# Timezones
tz_italy = pytz.timezone('Europe/Rome')
tz_higeco = pytz.timezone('Etc/GMT-1')
tz_utc = pytz.timezone('Etc/UTC')
dtfmt = '%Y-%m-%d %H:%M:%S'


# Methods to connect to the server

def verify_response(resp_status):
    if resp_status == 200:
        return True
    elif resp_status == 400:
        return False
    elif resp_status == 401:
        return False
    elif resp_status == 404:
        return False
    elif resp_status == 500:
        return False


def login():
    "Logs in using username and password and returns the token"
    path = "api/v1/authenticate"
    url = '{0}{1}'.format(host, path)
    header = {'username': username, 'password': password}
    # For login with api token if available
    #header = {'apiToken': apiToken}
    resp = session.request('POST',url,data=json.dumps(header))
    
    if verify_response(resp.status_code):
        resp_json = resp.json()
        token = resp_json['token']
        return token
    else:
        return False


def request(path):
    "Sends a GET request after logging in and returns a json object"
    token = login()
    header = {'authorization':token}
    url = '{0}{1}'.format(host, path)
    try:
        resp = session.request('GET',url,headers = header)
        if verify_response(resp.status_code):
            try:
                resp_json = resp.json()
                return resp_json
            except:
                pass
        else:
            return None
    except:
        return None


def dt_to_ts_tz(dt):
    "Converts datetime to timestamp"
    dt_loc = tz_utc.localize(datetime.strptime(dt, "%Y-%m-%d %H:%M:%S"))
    dt_Higeco = dt_loc.astimezone(tz_italy)
    dt_str = datetime.strftime(dt_Higeco,"%Y-%m-%d %H:%M:%S")
    ts = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").timestamp()
    return ts


# Higeco API responses

def get_plant_list():
    path = "api/v1/plants"
    resp = request(path)
    return resp


def get_plant_description(plantId):
    path = "api/v1/plants/" + str(plantId) 
    resp = request(path)
    return resp


def get_device_list(plantId):
    path = "api/v1/plants/" + str(plantId) + "/devices"
    resp = request(path)
    return resp


def get_device_description(plantId, devId):
    path = "api/v1/plants/" + str(plantId) + "/devices/" + str(devId)
    resp = request(path)
    return resp


def get_logs_list(plantId, devId):
    path = "api/v1/plants/" + str(plantId) + "/devices/" + str(devId) + "/logs"
    resp = request(path)
    return resp


def get_logs_description(plantId, devId, logId):
    path = "api/v1/plants/" + str(plantId) + "/devices/" + str(devId) + "/logs/" + str(logId)
    resp = request(path)
    return resp


def get_items_list(plantId, devId, logId):
    path = "api/v1/plants/" + str(plantId) + "/devices/" + str(devId) + "/logs/" + str(logId) + "/items"
    resp = request(path)
    return resp


def get_items_description(plantId, devId, logId, itemId):
    path = "api/v1/plants/" + str(plantId) + "/devices/" + str(devId) + "/logs/" + str(logId) + "/items/" + str(itemId)
    resp = request(path)
    return resp


def get_log_data(plantId, devId, logId,
                 from_dt = datetime.strftime(datetime.now()+timedelta(hours=-24),"%Y-%m-%d %H:%M:%S"),
                 to_dt = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"),
                 sampTime = 0, maxSampN = 100000
                ):
    """Gets data from a given log and returns a json object. By default last 24 hours are returned.
    plantId: plant ID
    devId: device ID
    logId: log ID
    itemId: item ID
    from_dt: starting time string in the format YYYY-MM-DD HH:MM:SS
    to_dt: ending time stringin the format YYYY-MM-DD HH:MM:SS
    sampTime: sampling time of data
    maxSampN: Maximum number of samples to get."""

    from_ts = dt_to_ts_tz(from_dt)
    to_ts = dt_to_ts_tz(to_dt)

    params = "?from="+str(int(from_ts))+"&to="+str(int(to_ts))+"&samplingTime="+str(sampTime)+"&maxSampleNumber="+str(maxSampN)
    path = "api/v1/getLogData/" + str(plantId) + "/" + str(devId) + "/" + str(logId) + params
    resp = request(path)
    return resp


def get_item_data(plantId, devId, logId, itemId,
                  from_dt = datetime.strftime(datetime.now()+timedelta(hours=-24),"%Y-%m-%d %H:%M:%S"),
                  to_dt = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"),
                  sampTime = 0, maxSampN = 100000
                 ):
    """Gets data from a given item / variable and returns a json object. By default last 24 hours are returned.
    plantId: plant ID
    devId: device ID
    logId: log ID
    itemId: item ID
    from_dt: starting time string in the format YYYY-MM-DD HH:MM:SS
    to_dt: ending time stringin the format YYYY-MM-DD HH:MM:SS
    sampTime: sampling time of data
    maxSampN: Maximum number of samples to get."""

    from_ts = dt_to_ts_tz(from_dt)
    to_ts = dt_to_ts_tz(to_dt)

    params = "?from="+str(int(from_ts))+"&to="+str(int(to_ts))+"&samplingTime="+str(sampTime)+"&maxSampleNumber="+str(maxSampN)
    path = "api/v1/getLogData/" + str(plantId)+"/"+str(devId)+"/"+str(logId)+"/"+str(itemId) + params
    resp = request(path)
    return resp


def get_log_last_values(plantId, devId, logId):
    path = "api/v1/getLastValue/" + str(plantId) + "/" + str(devId) + "/" + str(logId)
    resp = request(path)
    return resp


def get_item_last_value(plantId, devId, logId, itemId):
    path = "api/v1/getLastValue/" + str(plantId) + "/" + str(devId) + "/" + str(logId) + "/" + str(itemId)
    resp = request(path)
    return resp


def get_alarms(from_dt=datetime.strftime(datetime.now()+timedelta(hours=-24), "%Y-%m-%d %H:%M:%S"),
               to_dt=datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
               ):

    from_ts = dt_to_ts_tz(from_dt)
    to_ts = dt_to_ts_tz(to_dt)
        
    params = "?from="+str(int(from_ts))+"&to="+str(int(to_ts))
    path = "api/v1/alarms/" + params
    resp = request(path)
    return resp


def get_plant_alarms(plantId, 
                    from_dt = datetime.strftime(datetime.now()+timedelta(hours=-24),"%Y-%m-%d %H:%M:%S"),
                    to_dt = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                    ):
        
    from_ts = dt_to_ts_tz(from_dt)
    to_ts = dt_to_ts_tz(to_dt)
        
    params = "?from="+str(int(from_ts))+"&to="+str(int(to_ts))
    path = "api/v1/alarms/" + str(plantId) + "/" + params
    resp = request(path)
    return resp

# Data processing functions

def get_all_items(output_filename):
    "Collects all items / variables available for all plants and returns a csv file"
    plants = list()
    devices = list()
    logs = list()
    items = list()
    all_items = list()
    plant_list = get_plant_list()

    for plant in plant_list:
        plant_name = plant['name']
        plant_id = plant['id']
        plants.append((plant_name, plant_id))
        device_list = get_device_list(plant_id)
        for device in device_list:
            device_name = device['name']
            device_id = device['id']
            devices.append((device_name, device_id))
            log_list = get_logs_list(plant_id, device_id)
            for log in log_list:
                log_name = log['name']
                log_id = log['id']
                logs.append((log_name, log_id))
                item_list = get_items_list(plant_id, device_id, log_id)
                try:
                    for item in item_list:
                        item_name = item['name']
                        item_id = item['id']
                        items.append((item_name, item_id))
                        all_items.append((plant_name, plant_id, device_name,
                                          device_id, log_name, log_id, item_name, item_id))
                except:
                    pass

    with open(output_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quotechar="'", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(('Plant', 'Plant id', 'Device - GWC', 'Device id', 'Log - Section', 'Log id', 'Item - Variable', 'Item id'))
        for line in range(len(all_items)):
            writer.writerow(all_items[line])


def get_data_logs(filename, from_dt, to_dt, sample_time=900, maxSamp=1000000):
    """Gets data from a list of logs defined in an excel file as input and returns a dictionary of json objects for each log and a dataframe of the collected variables.
        Parameters:
            filename: Excel file that must contain the named columns: Plant id, Device id and Log id
            from_dt: starting time string in the format YYYY-MM-DD HH:MM:SS
            to_dt: ending time stringin the format YYYY-MM-DD HH:MM:SS
            sample_time: sampling time of the data
            maxSamp: Maximum number of samples to get"""
    variables = pd.read_excel(filename, dtype=str)
    idx = variables.index
    output = dict()
    for log in list(idx):
        plantId = variables['Plant id'][log]
        devId = variables['Device id'][log]
        logId = variables['Log id'][log]
        output[log] = get_log_data(plantId, devId, logId, from_dt, to_dt, sample_time, maxSamp)
    return(output, variables)


def get_data_items(filename, from_dt, to_dt, sample_time=900, maxSamp=1000000):
    """Gets data from a list of items defined in an excel file as input and returns a dictionary of json objects for each item and a dataframe of the collected variables.
        Parameters:
            filename: Excel file that must contain the named columns: 'Plant id', 'Device id', 'Log id' and 'Item id'
            from_dt: starting time string in the format YYYY-MM-DD HH:MM:SS
            to_dt: ending time stringin the format YYYY-MM-DD HH:MM:SS
            sample_time: sampling time of the data
            maxSamp: Maximum number of samples to get"""
    variables = pd.read_excel(filename, dtype=str)
    idx = variables.index
    output = dict()
    for var in list(idx):
        plantId = variables['Plant id'][var]
        devId = variables['Device id'][var]
        logId = variables['Log id'][var]
        itemId = variables['Item id'][var]
        output[var] = get_item_data(plantId, devId, logId, itemId, from_dt, to_dt, sample_time, maxSamp)
    return(output, variables)


def mround(x, m):
    "Rounds x to the nearest multiple m."
    return m * round(x / m)


def preprocess_data(dict_input, variables):
    """Receives an input dictionary and a variables dataframe and returns an items dataframe
    of the processed variables and the data alligned by datetime."""
    items = pd.DataFrame()
    data = pd.DataFrame()

    for log in dict_input:
        if log not in variables.index:
            print(log)
            continue
        try:
            # Read data from input dictionary
            df_log = pd.DataFrame.from_dict(dict_input[log]['log'], orient='index')
            df_items = pd.DataFrame(dict_input[log]['items'])
            df_data = pd.DataFrame(dict_input[log]['data'])

            # Set items df 
            df_items['log_id'] = df_log[0]['id']
            df_items['log_name'] = df_log[0]['name']
            df_items['device_id'] = variables['Device id'][log]
            df_items['device_name'] = variables['Device - GWC'][log]
            df_items['plant_id'] = variables['Plant id'][log]
            df_items['plant_name'] = variables['Plant'][log]
            items = items.append(df_items, sort=False)

            # Replace error value codes in Higeco as nans
            df_data.replace('#E2', np.nan, inplace=True)
            df_data.replace('#E3', np.nan, inplace=True)

            # Round datatime series to the nearest minute and remove duplicates
            df_data[0] = mround(df_data[0], 60)
            df_data[0] = pd.to_datetime(df_data[0], unit='s')
            df_data.drop_duplicates([0], inplace=True)

            # Rename columns to corresponding items ids
            df_data.columns = ['Datetime']+df_items.id.to_list()

            # Set datetime as index
            df_data = df_data.set_index('Datetime')

            # Remove empty columns
            df_data = df_data.dropna(how='all', axis=1)

            if data.empty:
                data = df_data
            else:
                data = data.join(df_data)
        except:
            pass

    # Organize items df
    items.rename(columns={'id': 'item_id', 'name': 'item_name', 'unit': 'item_unit', 'index': 'item_index'}, inplace=True)
    items_order = ['item_index', 'item_id', 'item_name', 'item_unit', 'log_id', 'log_name', 'device_id', 'device_name', 'plant_id', 'plant_name']
    items = items[items_order]

    # Organize data df and reset index to get datetime column in PowerBI
    data.columns = data.columns.astype('str')
    data.reset_index(inplace=True)
    data.sort_values(by='Datetime', inplace=True)
    return(items, data)


def resample_data(data_input, aggregation, resample_time='15T'):
    data_resamp = data_input.resample(resample_time).agg(aggregation)
    return(data_resamp)