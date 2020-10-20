from df_to_excel import append_df_to_excel
import pandas as pd
from datetime import datetime as dt
import requests
import sys
import platform
# import psutil
import json
import time

# API_url = "http://192.168.100.7/api"
API_url = "http://localhost:8000/api"
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
data = open("data_sql.json").read()
data = json.loads(data)

def db_prep():

    time.sleep(1)

    patient_request = requests.post(f'{API_url}/patients/', data=data['patient'])
    patient_response = patient_request.json()
    patient_id = patient_response['id']

    sensor_request = requests.post(f'{API_url}/sensors/', data=data['sensor'])
    sensor_response = sensor_request.json()

    sensors = []
    sensors.append(int(sensor_response['id']))
    data['equipment']['sensors'] = sensors

    equipment_request = requests.post(
        f'{API_url}/equipments/', data=data['equipment'])
    equipment_response = equipment_request.json()
    print(equipment_response)

    equipment_id = equipment_response['id']

    loan_data = {"equipment": equipment_id, "patient": patient_id}
    loan_history = requests.post(
        f'{API_url}/loans-history/', data=loan_data)

    collect_data = {
        "equipment": equipment_id,
        "patient": patient_id
    }
    collect_request = requests.post(f'{API_url}/collects/', data=collect_data)
    collect_response = collect_request.json()
    collect_id = int(collect_response['id'])


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

    return sample_data


def write_excel(start_timer, qt_bytes,
                final_timer, latency, operation, cenario, qt_requisicoes):

    latencia = str(latency)
    table_row = pd.DataFrame({
        "id_experimento": [str(start_timer).replace(':', '').replace('.', '')],
        "data": [dt.now().date()],
        "hora": [dt.now().time()],
        "I_O": [operation],
        "cenario": [cenario],
        "funcao_api": ["/samples"],
        "qt_bytes": [qt_bytes],
        "qt_requisicoesuisicoes": [qt_requisicoes],
        "time_stamp_init": [start_timer],
        "time_stamp_fin": [final_timer],
        "latencia ( J - I )": [str(latencia)]
    })

    append_df_to_excel(
        filename='experiment_sql.xlsx', df=table_row,
        sheet_name="Sheet1", header=False, index=False)



def write(MAX_RANGE, sample_data):
    qt_bytes = sys.getsizeof(sample_data) * MAX_RANGE

    iteration = 0
    sample_ids = []  # store sample ids in this array after each create

    # writing samples
    start_timer = dt.now()
    for iteration in range(MAX_RANGE):
        sample_request = requests.post(f'{API_url}/samples/', data=sample_data)
        print(sample_request.json())
        if(sample_request.status_code == 201):
            sample_ids.append(int(sample_request.json()['id']))
            iteration += 1
    FIN_TIMER = dt.now()
    latencia = FIN_TIMER - start_timer
    operation = 'I'
    cenario = 'single client / single write'

    write_excel(start_timer, qt_bytes, FIN_TIMER,
                latencia, operation, cenario, MAX_RANGE)

    print('finished write')
    return sample_ids


def read(MAX_RANGE, sample_ids, sample_data):
    qt_bytes = sys.getsizeof(sample_data) * MAX_RANGE

    # reading samples
    start_timer = dt.now()
    for _id in sample_ids:
        sample_request = requests.get(f'{API_url}/samples/{_id}/')
        if(sample_request.status_code != 200):
            print('Oooops, errorr')
    FIN_TIMER = dt.now()
    latencia = FIN_TIMER - start_timer
    operation = 'O'
    cenario = 'single cliente / single read'

    write_excel(start_timer, qt_bytes, FIN_TIMER,
                latencia, operation, cenario, MAX_RANGE)

    print('finished read')


def start_test(NUMBER_OF_TESTS, MAX_RANGE=100):
    for iteration in range(NUMBER_OF_TESTS):
        sample_data = db_prep()
        time.sleep(1)
        samples_id = write(MAX_RANGE, sample_data)
        time.sleep(1)
        read(MAX_RANGE, samples_id, sample_data)
        time.sleep(1)
        requests.delete(f"{API_url}/delete/")
        MAX_RANGE += 100


run_test = 'y'

while run_test.lower() == 'y':
    number_of_tests = int(input('number of tests: '))
    start_test(number_of_tests)
    run_test = input('Run tests again? Y or N: ')
