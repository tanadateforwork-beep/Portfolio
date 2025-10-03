import sys
import os
import configparser
from datetime import datetime
import paramiko
import ctds
import pandas as pd
current_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(f'/home/vanta/media/Production/slack_sender')
from slack_sender import send_message, send_file

# Create parser
config_file_path = f'{current_path}/../Config_server/Configuration.cfg'
config = configparser.ConfigParser()
config.read(config_file_path)

#Data Config 
vtc_trd_ip = config['Vtc-trd']['ip_server']
vtc_trd_user = config['Vtc-trd']['username']
vtc_trd_password = config['Vtc-trd']['password']

vantadata_ip = config['vantadata']['ip_server']
vantadata_username = config['vantadata']['username']
vantadata_password = config['vantadata']['password']

#Check today is holiday?
for i in range(5):
    try:
        conn = ctds.connect(server=vantadata_ip, user=vantadata_username, password=vantadata_password,
                            database='NASDAQ', autocommit=False, timeout=60000)
        c = conn.cursor()
        break
    except:
        pass
c.execute('SELECT Date FROM [VANTA].[dbo].[SET_Holiday]')
rows = c.fetchall()
# Convert the list of tuples to list of lists
rows = [list(row) for row in rows]
df_holiday = pd.DataFrame(rows, columns=['Date'])
df_holiday['Date'] = pd.to_datetime(df_holiday['Date'])
if datetime.now().date() in df_holiday['Date'].dt.date.values:
    print('Stop today is holiday.')
    conn.close()
    exit()

current_date = datetime.now().date()
str_current_date = current_date.strftime('%Y%m%d')
str_current_date_check = current_date.strftime('%Y-%m-%d')
times = ["0800","0930"]

# Create an SSH client
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    client.connect(vtc_trd_ip, username=vtc_trd_user, password=vtc_trd_password)
    sftp = client.open_sftp()
    file_found = False
    for t in times:
        path = f"/home/vanta1/AAT_vanta/nero_prod/setFifty/csv/IDset50_{str_current_date}_{t}.csv"
        # path = f"/home/vanta1/AAT_vanta/set50_prod/setFifty/csv/IDset50_{str_current_date}_{t}.csv"
        try:
            stat_result = sftp.stat(path)
            modified_time = stat_result.st_mtime
            file_dt = datetime.fromtimestamp(modified_time)
            threshold_dt = datetime.strptime(f"{str_current_date_check} 9:33:00", "%Y-%m-%d %H:%M:%S")
            if file_dt <= threshold_dt:
                str_modified_time = file_dt.strftime('%Y-%m-%d %H:%M:%S')
                message = f"Forgot to save changes to the trading list : {str_modified_time}"
                send_message('dw_alert', message)
                file_found = True
                break
            elif file_dt > threshold_dt:
                print(f"File modified time is greater than threshold: {file_dt} > {threshold_dt}")
                file_found = True
                break
        except FileNotFoundError:
            continue
    if not file_found:
        message = f"File not found for trading list on {str_current_date}."
        send_message('dw_alert',message)
        print(message)
finally:
    sftp.close()
    client.close()
    conn.close()