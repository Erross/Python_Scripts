#lambda function to take a newly added file and add each row to a dynamo db table

from __future__ import print_function
import csv
import boto3


def handler(event, context):
    dynamodb = boto3.resource('dynamodb')  # this breaks outside of lambda because of AWS creds
    table = dynamodb.Table(tablename)  # this breaks outside of lambda beause of AWS creds
    s3_client = boto3.client('s3')
    #First part saves the new file in S3 to a temp.txt file
    for record in event["Records"]:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        s3_client.download_file(bucket, key, '/tmp/temp.txt')
        myfile = open('/tmp/temp.txt', 'rb')

    lol = list(csv.reader(myfile, delimiter=','))
    headers = []
    headerspos = []
    #Here 'Inspection_Lot' is defined and a key value combining MIC and IL is created for insert into the Dynamo Table
    for index, x in enumerate(lol[0]):
        if x == 'Inspection Lot':
            x = x.replace(" ", "_")
            key = x
            keypos = "MIC_IL"
        elif x == MIC:
            key = MIC+"_"+key

        x = x.replace(" ", "_")
        headers.append(x + '= :' + x)
        headerspos.append(index)

    b = ','.join(headers)

    ujoin = 'set ' + b
    for row in lol[1:]:
        # START OF ROW PARSE
        keycode = {key: row[keypos]}
        loadbody = {}
        for val in headerspos:
            x = lol[0][val]
            x = x.replace(" ", "_")
            loadbody[":" + x] = row[val]

        try:
            table.update_item(
                Key=keycode,
                UpdateExpression=ujoin,
                ExpressionAttributeValues=loadbody,
                ReturnValues="UPDATED_NEW")
        except Exception, e:
            return e