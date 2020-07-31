from argparse import ArgumentParser
import os.path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pymongo
import datetime
from pymongo import MongoClient

#parser = argparse.ArgumentParser(description='generating NEEMs out of EASE CRC Research Data coming from PHANToM Haptic Device')
#parser.add_argument('integers', metavar='N', type=int, nargs='+',
#                   help='an integer for the accumulator')

def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, 'r')  # return an open file handle

parser = ArgumentParser(description='generating NEEMs out of EASE CRC Research Data coming from PHANToM Haptic Device')
parser.add_argument("-f", dest="filepath", required=True,
                    help="input h5 file containing ", metavar="H5FILE",
                    type=lambda x: is_valid_file(parser, x))
parser.add_argument("-i", dest="ip", required=True,
                    help="Mongo Server's IP", metavar="MONGO_IP",
                    type=open)

parser.add_argument("-p", dest="port", required=True,
                    help="Mongo Server's Port", metavar="MONGO_PORT",
                    type=int)

parser.add_argument("-n", dest="db_name", required=True,
                    help="Which database on Mongo", metavar="MONGO_DB_NAME",
                    type=open)

parser.add_argument("-t", dest="db_table", required=True,
                    help="Which table on the database", metavar="MONGO_DB_TABLE",
                    type=open)


args = parser.parse_args()


print('numpy: ', np.__version__)
print('pandas: ', pd.__version__)

client = MongoClient(args.ip, args.port)
db = client[args.db_name]
collection = db[args.db_table]

#client = MongoClient('localhost', 27017)
#db = client['ease-h']
#collection = db['trajectories']

hdf_file = pd.HDFStore(args.filepath, 'r')
trajectory_data=hdf_file['transport_trajectories']
hdf_file.close() # closes the file

trajectory_data.head()
old_trial = -1
inserter = {}

for ind, current_data in trajectory_data.sort_values(by=['participant', 'trial', 'ts']).iterrows():
    
    trial = current_data['trial']

    if old_trial != trial and old_trial != -1:
       # assert old dictionary
       collection_id = collection.insert_one(inserter).inserted_id
       inserter = {}

    if old_trial != trial:
       old_trial = trial
       inserter['trial'] = trial
       inserter['participant'] = current_data['participant']
       inserter['condition'] = current_data['condition']
       inserter['trial_i'] = current_data['trial_i']
       inserter['traj_x'] = []
       inserter['traj_y'] = []
       inserter['traj_z'] = []
       inserter['speed'] = []
       inserter['acc'] = []
       inserter['timestamps'] = []
       inserter['norm_ts'] = []
       inserter['SPARC'] = []
       inserter['LDLJ'] = []

    inserter['traj_x'].append(current_data['x'])
    inserter['traj_y'].append(current_data['y'])
    inserter['traj_z'].append(current_data['z'])
    inserter['speed'].append(current_data['speed'])
    inserter['acc'].append(current_data['acc'])
    inserter['timestamps'].append(current_data['ts'])
    inserter['norm_ts'].append(current_data['norm_ts'])
    inserter['SPARC'].append(current_data['SPARC'])
    inserter['LDLJ'].append(current_data['LDLJ'])





