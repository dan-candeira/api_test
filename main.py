from df_to_excel import append_df_to_excel
import pandas as pd
from datetime import datetime as dt
import requests
import sys
import platform
# import psutil
import json
import time

API_url = "http://192.168.100.7/api"
csv_file = 'lazaro_01_r.txt'



def format_header(header):
    formated_header = []
    # formating header
    for index, col in header.iteritems():
        name = col.name.replace('.', '_').replace(
            '[', '').replace(']', '').lower().strip()
        formated_header.append(name)
    return formated_header


# read the csv file and preparing the data

# loading and formating the df header
tmp_header = pd.read_csv(csv_file, header=1, nrows=0, 
                            delimiter="\t", sep="\t")
header = format_header(tmp_header)

# loading the df data
df = pd.read_csv(csv_file, skiprows=2, 
                    delimiter="\t", header=None)

df = df.drop(columns=41, axis=1)

# adding the header to the df
df.columns = header

# reading the json file with pre registered data
data = open("data.json").read()
data = json.loads(data)


time.sleep(1)

patient_request = requests.post(f'{API_url}/patient/', data = data['patient'])
patient_response = patient_request.json()
patient_id = patient_response['_id']

sensor_type_request = requests.post(f'{API_url}/sensor-type/', data = data['sensorType'])
sensor_type_response = sensor_type_request.json()

sensor_type_id = sensor_type_response['_id']
data['sensor']['types'] = [sensor_type_id,]

sensor_request = requests.post(f'{API_url}/sensor/', data = data['sensor'])
sensor_response = sensor_request.json()

sensors = []
sensors.append(sensor_response['_id'])
data['equipment']['sensors'] = sensors

equipment_request = requests.post(f'{API_url}/equipment/', data = data['equipment'])
equipment_response = equipment_request.json()

equipment_id = equipment_response['_id']

loan_data = {"equipment": equipment_id, "patient": patient_id}
requests.post(
    f'{API_url}/loan-history/', data=loan_data)

collect_data = {
    "equipment": equipment_id
}
collect_request = requests.post(f'{API_url}/collect/', data=collect_data)
collect_response = collect_request.json()
collect_id = collect_response['_id']


temp = 36.5
sample_header = ['time', 'ac_x', 'ac_y', 'ac_z', 'g_x', 'g_x', 'g_z', 'temp']

captured_data = []
captured_data.append([ 
        df[['time']].iloc[1]['time'],  
        df[['a1_x']].iloc[1]['a1_x'], 
        df[['a1_y']].iloc[1]['a1_y'], 
        df[['a1_z']].iloc[1]['a1_z'],
        df[['g1_x']].iloc[1]['g1_x'],
        df[['g1_y']].iloc[1]['g1_y'],
        df[['g1_z']].iloc[1]['g1_z'],
        temp
    ])


sample_data = {
    'header': sample_header,
    'captured_data': captured_data,
    'collect': collect_id
}


def write_excel(START_TIMER, BYTE_SIZE, 
                FINAL_TIMER, LATENCY, OPERATION, CENARIO, QT_REQ):

    hardware_config = f'''
    {platform.uname()}
    '''


    software_config = f'''
    total_disco: 30gb,
    ram: 2gb
    '''


    table_row = pd.DataFrame({
    "id_experimento": str(START_TIMER).replace(':', '').replace('.' ,''), 
    "data": dt.now().date(),
    "hora": dt.now().time(),
    "I_O": OPERATION, 
    "cenario": CENARIO, 
    "configuracao_hard": hardware_config, 
    "configuracao_soft": software_config, 
    "funcao_api": "/sample", 
    "qt_bytes": BYTE_SIZE, 
    "qt_requisicoes": QT_REQ,
    "time_stamp_init": START_TIMER, 
    "time_stamp_fin": FINAL_TIMER, 
    "latencia ( J - I )": str(LATENCY)
    })

    append_df_to_excel(
    filename='experiment_sheet.xlsx',df=table_row, 
    sheet_name="Sheet1", header=False)



sample_ids = [] # store sample ids in this array after each create


def write(MAX_RANGE):
    BYTE_SIZE = sys.getsizeof(sample_data) * MAX_RANGE

    iteration = 0

    # writing samples
    START_TIMER = dt.now()
    for iteration in range(MAX_RANGE):
        sample_request = requests.post(f'{API_url}/sample/', data = sample_data)
        if(sample_request.status_code == 201):
            sample_ids.append(sample_request.json()['_id'])
            iteration+=1
    FIN_TIMER = dt.now()
    LATENCY = FIN_TIMER - START_TIMER
    OPERATION='I'
    CENARIO='write'
    QT_REQ=MAX_RANGE

    write_excel(START_TIMER, BYTE_SIZE, FIN_TIMER, LATENCY, OPERATION, CENARIO,QT_REQ )

    print('finished write')


def read(MAX_RANGE):
    BYTE_SIZE = sys.getsizeof(sample_data) * MAX_RANGE

    # reading samples
    START_TIMER = dt.now()
    for _id in sample_ids:
        sample_request = requests.get(f'{API_url}/sample/{_id}')
        if(sample_request.status_code != 200):
            print('Oooops, errorr')
    FIN_TIMER = dt.now()
    LATENCY = FIN_TIMER - START_TIMER
    OPERATION='O'
    CENARIO='read'
    QT_REQ=MAX_RANGE

    write_excel(START_TIMER, BYTE_SIZE, FIN_TIMER, LATENCY, OPERATION, CENARIO,QT_REQ)

    print('finished read')


def start_test(NUMBER_OF_TESTS, MAX_RANGE=100):
    for iteration in range(NUMBER_OF_TESTS):
        time.sleep(1)
        write(MAX_RANGE)
        time.sleep(1)
        read(MAX_RANGE)
        time.sleep(1)
        requests.delete(f"{API_url}/delete-all/1")
        MAX_RANGE += 100


run_test = input('Run tests? Y or N: ')

while run_test.lower() == 'y':
    number_of_tests = int(input('number of tests: '))
    start_test(number_of_tests)