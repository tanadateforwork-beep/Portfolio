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


#Config 
vtc_log_ip = config['Vtc-log-setnet']['ip_server']
vtc_log_user = config['Vtc-log-setnet']['username']
vtc_log_password = config['Vtc-log-setnet']['password']

vantadata_ip = config['vantadata']['ip_server']
vantadata_username = config['vantadata']['username']
vantadata_password = config['vantadata']['password']

#setparameter
current_date = datetime.today().strftime('%Y%m%d')
current_date2 = datetime.today().strftime('%Y-%m-%d')
current_time = datetime.now().time()
threshold_time_mrn = datetime.strptime("09:02:00", "%H:%M:%S").time()
threshold_time_lunch = datetime.strptime("12:05:00", "%H:%M:%S").time()
threshold_time_after_lunch = datetime.strptime("14:00:00", "%H:%M:%S").time()
threshold_time_afternoon = datetime.strptime("14:02:00", "%H:%M:%S").time()
threshold_time_eve = datetime.strptime("16:00:00", "%H:%M:%S").time()
remote_path_file = f'/home/vanta1/tcpdump/ouch-itch{current_date}.pcap'

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
    
# Create an SSH client
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    # Connect to the server
    client.connect(vtc_log_ip, username=vtc_log_user, password=vtc_log_password)
    
    # Command to check if the file exists
    command_check = f'if [ -f {remote_path_file} ]; then echo "exists"; else echo "not found"; fi'
    stdin, stdout, stderr = client.exec_command(command_check)
    file_status = stdout.read().decode().strip()

    if file_status == "not found":
        message = "Please Check Ouch-Itch is not found!!"
        send_message('production_connection_alert',message)
        current_file_size = 0
    else:
        # Command to get the file size
        command = f'stat -c %s {remote_path_file}'
        # Execute the command
        stdin, stdout, stderr = client.exec_command(command)
        # Read the output
        file_size = stdout.read().decode().strip()
        current_file_size = int(file_size) 
    
    if current_time < threshold_time_lunch:
        path_file = f'{current_path}/../filesize_ouchitch/ouchitch_check_mrn_{current_date2}.csv'
    elif current_time < threshold_time_eve:
        path_file = f'{current_path}/../filesize_ouchitch/ouchitch_check_eve_{current_date2}.csv'
    print(f"Current File Size: {current_file_size} bytes")
    if current_time < threshold_time_mrn :
        df = pd.DataFrame({'File Size': [current_file_size]})
        df.to_csv(path_file, index=False)
        print("The First time check Data Produce In PCAP file")
    elif current_time > threshold_time_mrn and current_time < threshold_time_lunch:
        df = pd.read_csv(path_file)
        previous_file_size = df['File Size'].iloc[0]
        if current_file_size > previous_file_size:
            print("Intraday check Data Produce In PCAP file")
            df = pd.DataFrame({'File Size':[current_file_size]})
            df.to_csv(path_file,index=False)
        elif current_file_size == previous_file_size:
            message = "Please Check Ouch-Itch!! No Data Produce In PCAP file"
            send_message('production_connection_alert',message)

    if current_time>=threshold_time_after_lunch and current_time < threshold_time_afternoon:
        df = pd.DataFrame({'File Size': [current_file_size]})
        df.to_csv(path_file, index=False)
        print("Data Produce In PCAP file")
    elif current_time > threshold_time_afternoon and current_time < threshold_time_eve:
        df = pd.read_csv(path_file)
        previous_file_size = df['File Size'].iloc[0]
        if current_file_size > previous_file_size:
            print("Intraday check Data Produce In PCAP file")
            df = pd.DataFrame({'File Size':[current_file_size]})
            df.to_csv(path_file,index=False)
        elif current_file_size == previous_file_size:
            message = "Please Check Ouch-Itch!! No Data Produce In PCAP file"
            send_message('production_connection_alert',message)
    
    conn.close()
    client.close()

except Exception as e:
    print(f"Error: {e}")
