#Code to pull from a quickbase table and write to S3 - uses quickbase_credentials in a seperate lambda to return creds
#resourcename = name of the resource on quickbase
#tablename = name of table in resource (from url)
#apptoken = apptoken generated for API use - by quickbase
import pandas as pd
import requests
from xml.etree import ElementTree as ET
import time
import boto3
import sys
import json





def lambda_handler(event, context):
    lambda_client = boto3.client('lambda')
    invoke_response = lambda_client.invoke(FunctionName="get_creds",
                                           InvocationType='RequestResponse'
                                           )

    Ticket = (json.loads(invoke_response['Payload'].read()))

    ##############Run the query to return all exceptions################
    ##############Returns all exceptions *after* 5/1/2017 (milliseconds since epoch value used, because why not)##############
    myurl = 'https://'+resourcename+'seedbin.quickbase.com/db/'+tablename+'?a=API_DoQuery&includeRids=1&slist=3&ticket=' + Ticket + '&apptoken='+apptoken
    r = requests.post(myurl)
    tree = ET.fromstring(r.content)

    all_records = []
    for child in tree.findall('record'):
        record = {}
        # print child.tag
        for x in child:
            if x.tag == "date_created" or "planner_request" in x.tag or "exception" in x.tag and x.text is not None:
                try:
                    record[x.tag] = time.strftime("%m-%d-%Y", time.gmtime(float(float(x.text) / 1000)))
                except:
                    record[x.tag] = x.text
            else:
                record[x.tag] = x.text

            all_records.append(record)

    End_result = pd.DataFrame(all_records)
    End_result = End_result.drop_duplicates()
    try:
        End_result_dim = End_result.shape
        End_result_dim = ''.join(map(str,End_result_dim))
    except:
        End_result_dim = '0,0'
    End_result.to_csv(r"/tmp/output.csv", sep=',', index=False, header=True)
    try:
        Filewrite('output.csv','/tmp/output.csv',S3_bucket_name)
        return"success "+End_result_dim
    except:
        e = sys.exc_info()[0]
        return"You suck at python, try harder "+End_result_dim+" Error = "+e

def Filewrite(s3filename,sourcefile,bucket):
    s3 = boto3.resource('s3')
    s3.Object(bucket, s3filename).put(Body=open(sourcefile, 'rb'))