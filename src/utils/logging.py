import os
from datetime import datetime

def record_data_refresh_log(table):
    
    now = datetime.now()
    filename = f'./data/logs/{table}.txt'
    
    current_dt = now.strftime("%Y-%m-%d %H:%M:%S")
    data = f"Data for {table} refreshed at " + current_dt
     
    if os.path.isfile(filename):
        with open(filename, 'a') as f:          
            f.write('\n' + data)   
    else:
        with open(filename, 'w') as f:                   
            f.write(data)
