import sys
import os
import configparser
from datetime import datetime
import paramiko
import ctds
import pandas as pd
current_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(f'{current_path}/../slack_sender')
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

#setparameter
current_date = datetime.today().strftime('%Y%m%d')
current_date2 = datetime.today().strftime('%Y-%m-%d')
remote_path_file_oe = f'/home/vanta1/AAT_vanta/nero_prod/log/{current_date}/set_oe_raw_{current_date2}.log' 
remote_path_file_op = f'/home/vanta1/AAT_vanta/nero_prod/log/{current_date}/set50dw_op_log_{current_date2}.log'
# remote_path_file_oe = f'/home/vanta1/AAT_vanta/set50_prod/log/{current_date}/set_oe_raw_{current_date2}.log' 
# remote_path_file_op = f'/home/vanta1/AAT_vanta/set50_prod/log/{current_date}/set50dw_op_log_{current_date2}.log'
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

current_time = datetime.now().time()
time_mrn = datetime.strptime("10:04:59", "%H:%M:%S").time()
time_lunch = datetime.strptime("12:32:00", "%H:%M:%S").time()
time_afternoon = datetime.strptime("14:00:00", "%H:%M:%S").time()
if current_time < time_mrn:
    print('Wait to trade')
    c.close()
    conn.close()
    exit()
if current_time > time_lunch and current_time < time_afternoon:
    print('Break Lunch')
    c.close()
    conn.close()
    exit()
# Create an SSH client
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    # Connect to the server
    client.connect(vtc_trd_ip, username=vtc_trd_user, password=vtc_trd_password)

    # Command to get the file size
    command_oe = f'stat -c %y {remote_path_file_oe}'
    command_op = f'stat -c %y {remote_path_file_op}'
    
    # Execute the command for the first file
    stdin, stdout, stderr = client.exec_command(command_oe)
    file_timestamp_oe = stdout.read().decode().strip()
    if file_timestamp_oe == "not found":
        message = "Please Check set_oe_raw is not found!!"
        send_message('production_connection_alert',message)
        current_file_size = 0
        conn.close()

    # Execute the command for the second file
    stdin, stdout, stderr = client.exec_command(command_op)
    file_timestamp_op = stdout.read().decode().strip()
    if file_timestamp_op == "not found":
        message = "Please Check set50dw_op_log is not found!!"
        send_message('production_connection_alert',message)
        current_file_size = 0
        conn.close()

    # Truncate the fractional seconds to six digits
    file_timestamp_oe = file_timestamp_oe[:file_timestamp_oe.rfind('.') + 7] + file_timestamp_oe[file_timestamp_oe.rfind(' '):]
    file_timestamp_op = file_timestamp_op[:file_timestamp_op.rfind('.') + 7] + file_timestamp_op[file_timestamp_op.rfind(' '):]

    # Convert the timestamps to datetime objects
    timestamp_oe = datetime.strptime(file_timestamp_oe, '%Y-%m-%d %H:%M:%S.%f %z')
    timestamp_op = datetime.strptime(file_timestamp_op, '%Y-%m-%d %H:%M:%S.%f %z')

    # Calculate the time difference
    time_difference = (timestamp_op - timestamp_oe).total_seconds() / 60  # in minutes
    # Check if the difference is greater than 5 minutes
    if time_difference > 5:
        message = f"Alert Raptor is Down! The modification times of the oe and op files are not nearly identical.: {time_difference} minutes"
        send_message('production_connection_alert',message)
    else:
        print(f"The modification times of the oe and op files are nearly identical : {time_difference} minutes")
        # message = f"The modification times of the oe and op files are nearly identical : {time_difference} minutes"
        # send_message('production_connection_alert',message)
finally:
    client.close()
    conn.close()
