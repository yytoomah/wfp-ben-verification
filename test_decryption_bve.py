from getpass import getpass
from operator import truediv
import os
import sched
from mysql.connector import connect, Error
import mysql.connector
import schedule, time
from datetime import datetime
from sys import exit
import boto3
import yaml
import gc
import pandas

def get_params():
    get_input_params = "SELECT * FROM credentials"
    try:
        with connect(
            host='127.0.0.1',
            user='root',
            password='Mysql@2022',
            database='bve_input_args'
        ) as connection:
            print(connection)
            with connection.cursor() as cursor:
                cursor.execute(get_input_params)
                result = cursor.fetchall()
                for row in result:
                    inputs = row
                    print(row)     
    except Error as e:
        print(e)
    # print(inputs)

    path_to_briefcase = inputs[2]
    #print(path_to_briefcase)
    username = inputs[0]
    #print(username)
    password = inputs[1]
    #print(password)
    storage_path = inputs[3]
    #print(storage_path)
    export_path = inputs[5]
    #print(export_path)
    form_id = inputs[4]
    #print(form_id)
    path_to_private_key = inputs[6]
    #print(path_to_private_key)
    project_url =  inputs[7]
    #print(project_url)
    access_key =  inputs[9]
    secret_access_key =  inputs[10]

    return inputs

def export_data(config):
    print('start')
    os.system("C:\\Users\\youssef.toomah\\Documents\\test_data_export.bat " 
                + config['project_url'] + " "
                + config['path_to_briefcase'] + " "               
                + config['username'] + " "
                + config['password'] + " "
                + config['storage_path'] + " "
                + config['form_id'] + " "
                + config['export_path'] + " "
                + datetime.now().strftime("%Y%m%d%H%M%S") + " "
                + config['path_to_private_key'] + " "
                + datetime.now().strftime("%Y-%m-%d"))
                
    write_csv_to_s3(config)
    

def write_csv_to_s3(config):
    s3_resource = boto3.resource('s3',aws_access_key_id=config['access_key'],aws_secret_access_key=config['secret_access_key'])
    for file in os.listdir('C:\\1\\export'):
        if file.endswith('.csv') and str.startswith(file,datetime.now().strftime("%Y%m%d")) and len(pandas.read_csv('C:\\1\\export\\' + file)) > 1:            
             path_to_export_file = 'C:\\1\\export\\' + file
             print(path_to_export_file)
             s3_resource.Object('389714470976-wfpnest-staging-dattestout',file).upload_file(path_to_export_file)
             os.remove(path_to_export_file)
        else:
            print('not submissions file')
    print('end')
    print(datetime.now())

with open('C:\\Users\\youssef.toomah\\Documents\\config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
s = sched.scheduler(time.time, time.sleep)
schedule.every().day.at("17:33").do(export_data,config)

while True:
    schedule.run_pending()
    time.sleep(10)
    gc.collect()
    

