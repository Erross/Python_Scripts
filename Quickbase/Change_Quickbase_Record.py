###This script takes a file from S3 and parses it then adds the new data to the specified table (tablename) in the database (resource) by way of the application (apptoken)
###only accepts a single row - each record to be added should be dropped into S3 as a single row csv file - lambda will deal with the rest
###Uses authentication lambda function

#########imports
from __future__ import print_function
import requests
import time
import boto3
import csv
import json

def handler(event, context):
    ####Authenticate######
    lambda_client = boto3.client('lambda')
    invoke_response = lambda_client.invoke(FunctionName="get_creds",
                                           InvocationType='RequestResponse'
                                           )

    Ticket = (json.loads(invoke_response['Payload'].read()))

    #####Read input text file and create the strings to enter into URL request here

    s3_client = boto3.client('s3')

    for record in event["Records"]:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        print(bucket)
        print(key)
        s3_client.download_file(bucket, key, '/tmp/temp.txt')
        file = open('/tmp/temp.txt', 'rb')
        lol = list(csv.reader(file, delimiter='\t'))

    #Here planner request and leadership exception were used as the columns to be changed, this will need to be revised on a use case basis
    if lol[1][2] == 'Accept':
        x = lol[1][1]
        x = x.replace(" - ", "___")
        x = x.replace("planner request", "leadership_exception")
        x = x.replace(" ", "_")
        print(x)
    #Add record here rather than edit - quickbase uses a display last thing fxn
    if lol[1][2] == 'Accept':
        myurl = 'https://'+resource+'.quickbase.com/db/'+tablename+'?a=API_AddRecord&ticket=' + Ticket + '&_fnm_inspection_lot=' + lol[1][0]+'&_fnm_leadership_status='+lol[1][2]+'&_fnm_'+x+'='+time.strftime("%m-%d-%Y", time.gmtime(float(float((int(time.time()))))))+'&apptoken='+apptoken
    else:
        myurl = 'https://'+resource+'.quickbase.com/db/'+tablename+'?a=API_AddRecord&ticket=' + Ticket + '&_fnm_inspection_lot=' + lol[1][0]+'&_fnm_leadership_status='+lol[1][2]+'&apptoken='+apptoken

    print(myurl)
    r = requests.post(myurl)
    return(r.content)