import pandas as pd
import json


def get_leader_data():
    df = pd.read_json("logs/server-1.log")
    # Convert individual columns with microsecond timestamps to datetime
    df['net_receive'] = pd.to_datetime(df['net_receive'], unit='us')
    df['channel_receive'] = pd.to_datetime(df['channel_receive'], unit='us')
    df['commit'] = pd.to_datetime(df['commit'], unit='us')
    df['sending_response'] = pd.to_datetime(df['sending_response'], unit='us')

    # Function to convert list of dicts with 'time' key in microseconds
    def convert_time_in_list_of_dicts(list_of_dicts):
        for d in list_of_dicts:
            d['time'] = pd.to_datetime(d['time'], unit='us')
        return list_of_dicts

    # Apply the conversion to 'send_accdec' and 'receive_accepted' columns
    df['send_accdec'] = df['send_accdec'].apply(convert_time_in_list_of_dicts)
    df['receive_accepted'] = df['receive_accepted'].apply(convert_time_in_list_of_dicts)
    return df

df = get_leader_data()
print(df.receive_accepted)
