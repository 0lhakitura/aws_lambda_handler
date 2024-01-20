import json
import boto3

s3_client = boto3.client('s3', aws_access_key_id='ACCESS_KEY_ID',
                         aws_secret_access_key='ACCESS_SECRET_KEY')
dynamodb = boto3.client('dynamodb', region_name='us-east-1')


def lambda_handler(event, context):
    source_bucket_name = event['Records'][0]['s3']['bucket']['name']
    csv_file_name = event['Records'][0]['s3']['object']['key']
    file_object = s3_client.get_object(Bucket=source_bucket_name, Key=csv_file_name)
    file_content = file_object['Body'].read().decode("utf-8-sig")
    oscars = file_content.splitlines()

    count = 0
    iteroscars = iter(oscars)
    next(iteroscars)

    for row in iteroscars:
        data = row.split(",")
        if (len(data)) >= 5:
            print("INVALID ROW: ", data, "ID: ", count + 1, sep='\n')
        award = str.lower(data[0])
        movie = ''.join(('\"', data[1], '\"'))
        data.pop(3)
        split = data[2].split(' ')
        first_name = split[0]
        data.append(str(split[1:])[2:-2])
        last_name = data[3]
        count = count + 1

        add_to_db = dynamodb.put_item(
            TableName='oscars',
            Item={
                'Id': {'N': str(count)},
                'award': {'S': str(award)},
                'movie': {'S': str(movie)},
                'first_name': {'S': str(first_name)},
                'last_name': {'S': str(last_name)},
            })

    transactionToUpload = {}
    transactionToUpload['file'] = csv_file_name
    transactionToUpload['status'] = 'successfully uploaded to DynamoDB'
    trigger_file_name = 'results' + '.json'
    uploadByteStream = bytes(json.dumps(transactionToUpload).encode('utf-8-sig'))
    s3_client.put_object(Bucket=source_bucket_name, Key='data/' + trigger_file_name, Body=uploadByteStream)