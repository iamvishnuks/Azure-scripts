import json
from azure.storage.blob import BlockBlobService
from azure.storage.blob import PublicAccess
import os, glob
from datetime import datetime
from elasticsearch import Elasticsearch,helpers

es = Elasticsearch([<esdata-node>])

storage_account = "<storage-account>"
account_key = "<storage-accountkey>"
container = "insights-logs-networksecuritygroupflowevent"
block_blob_service = BlockBlobService(account_name=storage_account, account_key=account_key)

#List blobs
generator = block_blob_service.list_blobs(container)

#Download locally
for blob in generator:
    print(blob.name)
    print("{}".format(blob.name))
    #check if the path contains a folder structure, create the folder structure
    if "/" in "{}".format(blob.name):
        print("there is a path in this")
        #extract the folder path and check if that folder exists locally, and if not create it
        head, tail = os.path.split("{}".format(blob.name))
        print(head)
        print(tail)
        if (os.path.isdir(os.getcwd()+ "/" + head)):
            #download the files to this directory
            print("directory and sub directories exist")
            block_blob_service.get_blob_to_path(container,blob.name,os.getcwd()+ "/" + head + "/" + tail)
        else:
            #create the diretcory and download the file to it
            print("directory doesn't exist, creating it now")
            os.makedirs(os.getcwd()+ "/" + head, exist_ok=True)
            print("directory created, download initiated")
            block_blob_service.get_blob_to_path(container,blob.name,os.getcwd()+ "/" + head + "/" + tail)
    else:
        block_blob_service.get_blob_to_path(container,blob.name,blob.name)

#Transformation
files = glob.glob("resourceId=/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*.json")
flow_tuples = []

for file in files:
    js = open(file,'r').read()
    js = json.loads(js)
    for records in js['records']:
        for flows in records['properties']['flows']:
            for flow in flows['flows']:
                for flow_tuple in flow['flowTuples']:
                    timestamp, src_ip, dst_ip, src_port, dst_port, protocol, traffic_flow, traffic_decision, flow_state, p_src_to_dst, b_source_to_dst, p_dst_to_src, b_dst_to_src = flow_tuple.split(',')
                    timestamp = datetime.fromtimestamp(int(timestamp))
                    if len(p_src_to_dst) == 0:
                        p_src_to_dst = 0
                    if len(b_source_to_dst) == 0:
                        b_source_to_dst = 0
                    if len(p_dst_to_src) == 0:
                        p_dst_to_src = 0
                    if len(b_dst_to_src) == 0:
                        b_dst_to_src = 0
                    if '.'.join(src_ip.split('.')[0:2]) == '.'.join(dst_ip.split('.')[0:2]):
                        continue
                    else:
                        flow_tuples.append({"_index":"nsg_flow_log", "_type": "document", "timestamp":timestamp, "src_ip":src_ip, "dst_ip":dst_ip, "src_port":src_port, "dst_port":dst_port,"protocol":protocol, "traffic_flow":traffic_flow, "traffic_decision":traffic_decision, "flow_state":flow_state, "p_src_to_dst":int(p_src_to_dst), "b_source_to_dst":int(b_source_to_dst), "p_dst_to_src":int(p_dst_to_src), "b_dst_to_src":int(b_dst_to_src) })


data_gen = (y for y in flow_tuples)

try:
  es.indices.create('nsg_flow_log')
except:
  print("already exists")

helpers.bulk(es, data_gen)
