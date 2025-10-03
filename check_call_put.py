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
# paths = {
#     "index": "/home/vanta1/AAT_vanta/set50_prod/setFifty/cfg/index",
#     "sellLmt": "/home/vanta1/AAT_vanta/set50_prod/setFifty/cfg/sellLmt",
#     "pt": "/home/vanta1/AAT_vanta/set50_prod/setFifty/cfg/pt",
#     "qty": "/home/vanta1/AAT_vanta/set50_prod/setFifty/cfg/qty"
# }
paths = {
    "index": "/home/vanta1/AAT_vanta/nero_prod/setFifty/cfg/index",
    "sellLmt": "/home/vanta1/AAT_vanta/nero_prod/setFifty/cfg/sellLmt",
    "pt": "/home/vanta1/AAT_vanta/nero_prod/setFifty/cfg/pt",
    "qty": "/home/vanta1/AAT_vanta/nero_prod/setFifty/cfg/qty"
}
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
    client.connect(vtc_trd_ip, username=vtc_trd_user, password=vtc_trd_password)
    sftp = client.open_sftp()

    for name, path in paths.items():
        try:
            files = sftp.listdir(path)
            if name == 'pt':
                if len(files) < 2:
                    Message = f"[{name}] No files in {path}"
                    send_message('production_connection_alert',Message)
            if files:
                for file in files:
                    full_path = f"{path}/{file}"
                    command = f'stat -c %s {full_path}'
                    stdin, stdout, stderr = client.exec_command(command)
                    output = stdout.read().decode().strip()
                    if output:
                        try:
                            file_size = int(output)
                            if file_size <= 10:
                                Message = f'Not productive file size : in {file}'
                                send_message('production_connection_alert',Message)
                            else:
                                print(f"[{name}] File size of {file} : {file_size} bytes")
                        except ValueError:
                            print(f"[{name}] Unable to parse file size for {file}")
                    else:
                        print(f"[{name}] No output from stat for {file}")
            else:
                Message = f"[{name}] No files in {path}"
                send_message('production_connection_alert',Message)
        except Exception as e:
            print(f"[{name}] Error accessing {path}: {e}")
finally:
    sftp.close()
    client.close()
    conn.close()
