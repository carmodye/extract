## Connecting to the database

## importing 'mysql.connector' as mysql for convenient
import mysql.connector as mysql
import json

## connecting to the database using 'connect()' method
## it takes 3 required parameters 'host', 'user', 'passwd'
db = mysql.connect(
    host="localhost",
    port="3307",
    user="root",
    passwd="secret",
    database="viens"
)


## json
class create_dict(dict):
    # __init__ function
    def __init__(self):
        self = dict()
        # Function to add key:value
    def add(self, key: object, value: object) -> object:
        """

        :type value: object
        """
        self[key] = value

## print(db)  # it will print a connection object if everything is fine

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
and activity.appid = apps.id limit 10"

## getting records from the table
cursor.execute(query)

## fetching all records from the 'cursor' object
records = cursor.fetchall()
num_fields = len(cursor.description)
field_names = [i[0] for i in cursor.description]
print(field_names)

mydict = create_dict()

for record in records:
    mydict.add(record[21],({"devicesID":record[0], \
                         "devicesUniqueid":record[1], \
                         "devicesWallid":record[2],
                         "devicesGroupid":record[3], \
                         "devicesUserid":record[4], \
                         "devicesAppid":record[5], \
                         "devicesName": record[6], \
                         "devicesLocation": record[7], \
                         "devicesZone": record[8], \
                         "devicesCity": record[9], \
                         "devicesState": record[10], \
                         "devicesDistrict": record[11],
                         "devicesRegion": record[12], \
                         "devicesTimezone": record[13],
                         "devicesVersion": record[14], \
                         "devicesLastquery": str(record[15]), \
                         "devicesLastupdate": str(record[16]), \
                         "devicesInterval": record[17], \
                         "activityId": record[18],\
                         "activityLocation": record[19],\
                         "activityTimestamp": str(record[20]),\
                         "activityEvent": record[21],\
                         "activityType": record[22],\
                         "activityWhat": str(record[23])
                         }))
    print(record)

stud_json = json.dumps(mydict, indent=2, sort_keys=True)

print(stud_json)

## Showing the data
## for record in records:
##    print(record)
