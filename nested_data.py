# loop through the columns in the csv
import psycopg2
from psycopg2 import Error
import json
import csv
import uuid
import db_config

node_ids = dict.fromkeys(["Administrative Subdivision Name","Administrative Subdivision Type"])

with open("test.mapping") as f:
    mapping_json = json.loads(f.read())
    print(len(mapping_json['nodes']))

for i in range(len(mapping_json['nodes'])):
    if mapping_json['nodes'][i]['arches_node_name'] in node_ids.keys():
        node_ids[mapping_json['nodes'][i]['arches_node_name']] = mapping_json['nodes'][i]['arches_nodeid']

graph_id = mapping_json['resource_model_id']

try:
    # Connect to an existing database
    connection_parameters = "host="+ db_config.host +" port="+db_config.port+" dbname="+db_config.dbname+" user="+db_config.user+" password="+db_config.password
    print('test',connection_parameters)
    conn = psycopg2.connect(connection_parameters)
    print("connection susccessful")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

cursor = conn.cursor()

with open("data.csv",'r') as file:
    reader=csv.reader(file)
    next(reader, None)
    parent_nodegroup_query_string = "select nodeid from nodes where graphid ='" + graph_id + "' and name = 'Place';"
    cursor.execute(parent_nodegroup_query_string)
    parent_nodegroup_id = cursor.fetchone()[0]

    child_nodegroup_query_string = "select nodeid from nodes where graphid ='" + graph_id + "' and name = 'Administration Subdivision';"
    cursor.execute(child_nodegroup_query_string)
    child_parent_nodegroup_id = cursor.fetchone()[0]

    resources= []
    whole_json = {}
    whole_json['business_data']={}
    for row in reader:
        resourceinstance = dict.fromkeys(["graph_id","legacyid","resourceinstanceid"])
        parenttile_id = uuid.uuid4()
        mahsa_id_node= 'e59cd612-63ed-11ec-8044-a72ed671fd8f'
        query_string = "select (tiledata::jsonb->'" + mahsa_id_node + "'), resourceinstanceid from tiles where replace((tiledata::jsonb->'" + mahsa_id_node + "')::varchar,'\"','') = '" + str(row[0]) + "';"
        cursor.execute(query_string)
        tiles = []
        if cursor.rowcount > 0:
            print('true')
            arches_uuid = cursor.fetchall()
            print ("resourceinstanceid is",arches_uuid)
            # create resourceinstance object
            resourceinstance["graph_id"]  = graph_id
            resourceinstance["legacyid"]  = arches_uuid[0][0]
            resourceinstance["resourceinstanceid"]  = arches_uuid[0][1]
            # create parent tile object
            parent_tile = dict.fromkeys(["data","nodegroup_id","parenttile_id","provisionaledits","resourceinstance_id","sortorder","tileid"])
            parent_tile["data"] = {}
            parent_tile["nodegroup_id"] = parent_nodegroup_id
            parent_tile["resourceinstance_id"] = arches_uuid[0][1]
            parent_tile["sortorder"] = 0
            parent_tile["tileid"] = str(uuid.uuid4())
            json.dumps(parent_tile)
            #create child tile object
            tiles.append(parent_tile) 
            for i in range(1,len(row),2):
                child_tile = dict.fromkeys(["data","nodegroup_id","parenttile_id","provisionaledits","resourceinstance_id","sortorder","tileid"])
                child_tile["nodegroup_id"] = child_parent_nodegroup_id
                child_tile["parenttile_id"] = parent_tile["tileid"]
                child_tile["resourceinstance_id"] = arches_uuid[0][1]
                child_tile["sortorder"] = 0
                child_tile["tileid"] = str(uuid.uuid4())
                data = {}
                data[node_ids["Administrative Subdivision Name"]] = row[i]
                data[node_ids["Administrative Subdivision Type"]] = row[i+1]  
                child_tile["data"] = data
                tiles.append(child_tile)
            single_resources = dict.fromkeys(['resourceinstance','tiles'])
            single_resources['resourceinstance'] = resourceinstance
            single_resources['tiles'] = tiles
        resources.append(single_resources)

with open('data.json', 'w') as f:
    whole_json['business_data']['resources'] = resources
    json.dump(whole_json, f)