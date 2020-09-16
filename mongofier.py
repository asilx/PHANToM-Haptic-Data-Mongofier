from argparse import ArgumentParser
import os.path
import numpy as np
import uuid
import pandas as pd
import matplotlib.pyplot as plt
import pymongo
import datetime
import time 
from pymongo import MongoClient
from owlready2 import *

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
                    help="input h5 file containing ", metavar="H5FILE")
parser.add_argument("-i", dest="ip", required=True,
                    help="Mongo Server's IP", metavar="MONGO_IP" )

parser.add_argument("-p", dest="port", required=True,
                    help="Mongo Server's Port", metavar="MONGO_PORT",
                    type=int)

parser.add_argument("-n", dest="db_name", required=True,
                    help="Which database on Mongo", metavar="MONGO_DB_NAME")

parser.add_argument("-t", dest="db_table", required=True,
                    help="Which table on the database", metavar="MONGO_DB_TABLE")


args = parser.parse_args()


print('numpy: ', np.__version__)
print('pandas: ', pd.__version__)

client = MongoClient(args.ip, args.port)
db = client[args.db_name]
collection = db[args.db_table]

#client = MongoClient('localhost', 27017)
#db = client['ease-h']
#collection = db['trajectories']

hdf_file_path = args.filepath
owl_file_path = hdf_file_path.replace("h5","owl")
hdf_file = pd.HDFStore(hdf_file_path, 'r')

trajectory_data=hdf_file['transport_trajectories']
hdf_file.close() # closes the file

#trajectory_data.head()

neem_onto = get_ontology("http://knowrob.org/kb/knowrob.owl")

with neem_onto:
    class PickAndPlace(Thing):
        namespace = neem_onto

    class TimePoint(Thing):
        namespace = neem_onto

    class startTime(ObjectProperty):
        namespace = neem_onto
        domain = [PickAndPlace]
        range = [TimePoint]

    class endTime(ObjectProperty):
        namespace = neem_onto
        domain = [PickAndPlace]
        range = [TimePoint]


old_trial = -1
inserter = {}
experiments = []
for ind, current_data in trajectory_data.sort_values(by=['participant', 'trial', 'ts']).iterrows():
    
    trial = current_data['trial']

    time_str = str(current_data['ts'])
    if len(time_str) == 19:
       time_str = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").timestamp()
    else:
       time_str = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f").timestamp()

    if old_trial != trial and old_trial != -1:
       # assert old dictionary
       end_time_str = str(inserter['timestamps'][len(inserter['timestamps']) - 1])
       #if len(time_str) == 19:
       #    time_str = time.mktime(datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").timetuple())
       #else:
       #    time_str = time.mktime(datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f").timetuple())
       endTime = TimePoint("TimePoint_" + end_time_str, namespace=neem_onto)
       experiments[len(experiments)-1].endTime = [endTime]
       collection_id = collection.insert_one(inserter).inserted_id
       inserter = {}

    if old_trial != trial:
       experiments.append(PickAndPlace("PickAndPlace_" + str(uuid.uuid4()), namespace=neem_onto))
       stTime = TimePoint("TimePoint_" + str(time_str), namespace=neem_onto)
       experiments[len(experiments)-1].startTime = [stTime]
       old_trial = trial
       inserter['trial'] = trial
       inserter['participant'] = current_data['participant']
       inserter['condition'] = current_data['condition']
       inserter['trial_i'] = current_data['trial_i']
       inserter['start'] = current_data['ts']
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
    #inserter['timestamps'].append(current_data['ts'])
    inserter['timestamps'].append(time_str)
    inserter['norm_ts'].append(current_data['norm_ts'])
    inserter['SPARC'].append(current_data['SPARC'])
    inserter['LDLJ'].append(current_data['LDLJ'])


neem_onto.save(file = owl_file_path, format = "rdfxml")



