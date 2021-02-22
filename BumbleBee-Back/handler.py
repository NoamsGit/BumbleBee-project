import os
import urllib
from pymongo import MongoClient
import boto3
from zipfile import ZipFile, is_zipfile
import io
import time


s3 = boto3.client('s3', aws_access_key_id='AKIAIYQG6AGFMAKBKPTA',
                  aws_secret_access_key='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')
cluster = MongoClient(
        'mongodb+srv://node-rest-shop:node-rest-shop@node-rest-shop.qy06n.mongodb.net/test?retryWrites=true&w=majority')



def parse_input(event, context):

    start_time = time.time()

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    input_file_name = key.split('.')[0]
    client_ID, date, powder_action = input_file_name.split('_')


    try:
        # extract coordinates
        coord_path = os.path.join(client_ID, date, powder_action, 'coordinates')
        coord_specific_path = s3.list_objects_v2(Bucket=bucket_name, Prefix=coord_path)['Contents'][0]['Key']
        response = s3.get_object(Bucket=bucket_name, Key=coord_specific_path)
        coord_text = io.StringIO(response["Body"].read().decode())
        coordinates = [line.split(',') for line in coord_text.readlines()]


        # extract humidity
        humidity_specific_path = os.path.join(client_ID, date, powder_action, 'arduino','temperature_humidity.txt')
        response = s3.get_object(Bucket=bucket_name, Key=humidity_specific_path)
        humidity_text = io.StringIO(response["Body"].read().decode())
        humidity = [line.split(',') for line in humidity_text.readlines()]


        # extract images
        images_name_list = []
        images_path = os.path.join(client_ID, date, powder_action, 'realsense')
        images_path_list = s3.list_objects_v2(Bucket=bucket_name, Prefix=images_path)['Contents']

        for zip in images_path_list:
            zip_name = zip['Key']
            response = s3.get_object(Bucket=bucket_name, Key=zip_name)
            zip_bytes = io.BytesIO(response['Body'].read())
            if is_zipfile(zip_bytes):
                zip_file = ZipFile(zip_bytes)
                name_list = zip_file.namelist()
                extensions = [".jpg", ".png", ".npy"]
                for image_name in name_list:
                    if any(match in image_name for match in extensions):
                        images_name_list +=  image_name
                        data = zip_file.open(image_name)
                        s3.put_object(Body=data, Bucket='bumblebee-bucket3',
                                      Key=image_name)




        # rgb_list = rgb_list + [img for img in images if img.startswith('rgb') and img.endswith('.png')]
        # infrared_list = infrared_list + [img for img in images if img.startswith('infrared')]
        # depth_list = depth_list + [img for img in images if img.startswith('depth') and img.endswith('.npy')]


        rgb_list = [name for name in images_name_list if name.startswith('rgb')]
        infrared_list = [name for name in images_name_list if name.startswith('infrared')]
        depth_list = [name for name in images_name_list if name.startswith('depth')]


        images = {'rgb': rgb_list, 'infrared': infrared_list, 'depth': depth_list}

        mongo_entry = {
            'client_ID': client_ID,
            'date': date,
            'powder_action': powder_action,
            'coordinates': coordinates,
            'humidity': humidity,
            'images': images
        }

        db = cluster["test"]
        collection = db["clients"]
        collection.insert_one(mongo_entry)

        end_time = time.time()
        print('proccess finish! ', end_time - start_time, ' sec')


        return 'Success!'

    except Exception as e:
        print(e)
        raise e


    # body = {
    #     "message": "Go NOAM Serverless v1.0! Your function executed successfully!",
    #     "input": event
    # }
    #
    # response = {
    #     "statusCode": 200,
    #     "body": json.dumps(body)
    # }
    # print(response)
    # return response


if __name__ == '__main__':
    parse_input('','')
