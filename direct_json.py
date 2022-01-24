## Connecting to the database

## importing 'mysql.connector' as mysql for convenient
import mysql.connector as mysql
import json
import numpy as np
import pymongo

## connecting to the database using 'connect()' method
## it takes 3 required parameters 'host', 'user', 'passwd'
db = mysql.connect(
    host="localhost",
    port="3307",
    user="root",
    passwd="secret",
    database="viens"
)

if db:
    print("Connected Successfully")
else:
    print("Connection Not Established")

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["events"]

## some json stuff
event_json_file = open("event.json", )
event_record = json.load(event_json_file)
event_json_file.close()

cursor = db.cursor()

## defining the Query
query: str = "SELECT devices.id as devicesID,  \
devices.uniqueid as devicesUniqueid,  \
devices.wallid as devicesWallid, \
devices.groupid as devicesGroupid, \
devices.userid as devicesUserid, \
devices.appid as devicesAppid, \
devices.name as devicesName, \
devices.location devicesLocation, \
devices.zone as devicesZone, \
devices.city as devicesCity, \
devices.state as devicesState, \
devices.district as devicesDistrict, \
devices.region as devicesRegion, \
devices.timezone as devicesTimezone,\
devices.version as devicesVersion, \
devices.lastquery as devicesLastquery, \
devices.lastupdate as devicesLastupdate, \
devices.interval as devicesInterval, \
activity.id as activityId, \
activity.location as activityLocation, \
activity.timestamp as activityTimestamp,\
activity.event as activityEvent,\
activity.type as activityType, \
activity.what as activityWhat \
FROM viens.devices, viens.activity, viens.apps \
where devices.uniqueid = activity.location \
and activity.timestamp > '2022-01-01' \
and activity.appid = apps.id limit 2"

## getting records from the table
cursor.execute(query)

## fetching all records from the 'cursor' object
records = cursor.fetchall()
num_fields = len(cursor.description)
field_names = [i[0] for i in cursor.description]
print(field_names)

#mydict = dict()

for record in records:
    mystr = str()
    mystr += "{ \"event\" : { "
    mystr += " \"devicesID\" : "
    mystr += "\""
    mystr += str(record[0])
    mystr += "\""
    mystr += ", \"devicesUniqueid\" : "
    mystr += "\""
    mystr += record[1]
    mystr += "\""
    mystr += ", \"devicesWallid\" : "
    mystr += "\""
    mystr += str(record[2])
    mystr += "\""
    mystr += ", \"devicesGroupid\" : "
    mystr += "\""
    mystr += str(record[3])
    mystr += "\""
    mystr += ", \"devicesUserid\" : "
    mystr += "\""
    mystr += str(record[4])
    mystr += "\""
    mystr += ", \"devicesAppid\" : "
    mystr += "\""
    mystr += str(record[5])
    mystr += "\""
    mystr += ", \"devicesName\" : "
    mystr += "\""
    mystr += record[6]
    mystr += "\""
    mystr += ", \"devicesLocation\" : "
    mystr += "\""
    mystr += record[7]
    mystr += "\""
    mystr += ", \"devicesZone\" : "
    mystr += "\""
    mystr += record[8]
    mystr += "\""
    mystr += ", \"devicesCity\" : "
    mystr += "\""
    mystr += record[9]
    mystr += "\""
    mystr += ", \"devicesState\" : "
    mystr += "\""
    mystr += record[10]
    mystr += "\""
    mystr += ", \"devicesDistrict\" : "
    mystr += "\""
    mystr += record[11]
    mystr += "\""
    mystr += ", \"devicesRegion\" : "
    mystr += "\""
    mystr += record[12]
    mystr += "\""
    mystr += ", \"devicesTimezone\" : "
    mystr += "\""
    mystr += str(record[13])
    mystr += "\""
    mystr += ", \"devicesVersion\" : "
    mystr += "\""
    mystr += record[14]
    mystr += "\""
    mystr += ", \"devicesLastquery\" : "
    mystr += "\""
    mystr += str(record[15])
    mystr += "\""
    mystr += ", \"devicesLastupdate\" : "
    mystr += "\""
    mystr += str(record[16])
    mystr += "\""
    mystr += ", \"devicesInterval\" : "
    mystr += "\""
    mystr += str(record[17])
    mystr += "\""
    mystr += ", \"activityId\" : "
    mystr += "\""
    mystr += str(record[18])
    mystr += "\""
    mystr += ", \"activityLocation\" : "
    mystr += "\""
    mystr += record[19]
    mystr += "\""
    mystr += ", \"activityTimestamp\" : "
    mystr += "\""
    mystr += str(record[20])
    mystr += "\""
    mystr += ", \"activityEvent\" : "
    mystr += "\""
    mystr += record[21]
    mystr += "\""
    mystr += ", \"activityType\" : "
    mystr += "\""
    mystr += record[22]
    mystr += "\""
    mystr += ", \"activityWhat\" : "
    mystr += "\""
    mystr += str(record[23]).replace('"','').replace('{','').replace('}','').replace(':','').replace(',','')
    mystr += "\""
    mystr += "} }"
    print(mystr)
    d = json.loads(mystr)
    myText = open(r'./' + str(record[18]) + '.json', 'w')
    myText.write(mystr)
    myText.close()
    print(record)

#stud_json = json.dumps(mydict, indent=2, sort_keys=True)
print(mystr)

#x = mycol.insert_many(stud_json)
#print list of the _id values of the inserted documents:
#print(x.inserted_ids)