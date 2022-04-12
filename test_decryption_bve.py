from asyncio import subprocess
from getpass import getpass
from operator import truediv
import os
import sched
import schedule, time
from datetime import datetime
from sys import exit
import boto3
import yaml
import gc
import pandas
import subprocess


def export_data(config):
    print('start')
    
    pro = ['java', '-jar', 
                    config['path_to_briefcase'], 
                    r'--pull_aggregate', 
                    r'--start_from_last', 
                    r'--odk_url',config['project_url'],
                    r'--odk_username',config['username'],
                    r'--odk_password',config['password'], 
                    r'--storage_directory',config['storage_path'],
                    r'--form_id',config['form_id']]

    print(pro)
    print('start pulling')
    res = subprocess.call(pro)
    print('submisisons pulled successfully')
    print(res)

    pro = ['java', '-jar', 
                    config['path_to_briefcase'], 
                    r'-e', 
                    r'-oc', 
                    r'-id',config['form_id'] ,
                    r'-sd',config['storage_path'],
                    r'-ed',config['export_path'], 
                    r'-f',datetime.now().strftime("%Y%m%d%H%M%S"),
                    r'-pf',config['path_to_private_key'],
                    r'-start',datetime.now().strftime("%Y-%m-%d")]

    print(pro)
    print('start exporting')
    res = subprocess.call(pro)
    print('submisisons exported to {export_path} successfully'.format(export_path=config['export_path']))
    print(res)

    print('save file in NEST')
    write_csv_to_s3(config)
    
    

def write_csv_to_s3(config):
    # create s3 connection
    s3_resource = boto3.resource('s3',aws_access_key_id=config['access_key'],aws_secret_access_key=config['secret_access_key'])
    # loop over all files in the specified directory
    for file in os.listdir(config['export_path']):
        # test if the file is csv & is created today & contains submissions
        if file.endswith('.csv') and str.startswith(file,datetime.now().strftime("%Y%m%d")) and len(pandas.read_csv(config['export_path']+ "\\"+ file)) > 1:            
             path_to_export_file = '{export_path}\t{file}'.format(export_path=config['export_path'],file=file) 
             print(path_to_export_file)
             s3_resource.Object('389714470976-wfpnest-staging-dattestout',file).upload_file(path_to_export_file)
             print('file successfully saved')
             os.remove(path_to_export_file)
        else:
            print('File could not be exported')
    print('end')
    print(datetime.now())

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
s = sched.scheduler(time.time, time.sleep)
schedule.every().day.at("12:53").do(export_data,config)

while True:
    schedule.run_pending()
    time.sleep(10)
    gc.collect()
    

