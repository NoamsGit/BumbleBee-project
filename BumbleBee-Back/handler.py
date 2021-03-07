import os
import urllib
from pymongo import MongoClient
import boto3
from zipfile import ZipFile, is_zipfile
import io
import time
from secret import AWS_KEY, AWS_SECRET_KEY,MONGODB_URL
from  utils.parsers import parse_coord_record, parse_humidity_record


 

s3 = boto3.client('s3', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
cluster = MongoClient(MONGODB_URL)
db = cluster["test"]
collection = db["clients"]

def parse_input(event, context):
    start_time = time.time()
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    input_file_name = os.path.dirname(key)
    client_ID, date, pollination_action = input_file_name.split('/')

    try:
        # extract coordinates
        coordinates = []
        coords_Prefix = os.path.join(client_ID, date, pollination_action, 'coordinates')
        coords_path_list = s3.list_objects_v2(Bucket=bucket_name, Prefix=coords_Prefix)['Contents']
        for coords_file in coords_path_list:
            response = s3.get_object(Bucket=bucket_name, Key=coords_file['Key'])
            coord_text = io.StringIO(response["Body"].read().decode())
            coordinates += [parse_coord_record(line.rstrip("\n")) for line in coord_text.readlines()]

        # extract humidity
        humidity_specific_path = os.path.join(client_ID, date, pollination_action, 'arduino','temperature_humidity.txt')
        response = s3.get_object(Bucket=bucket_name, Key=humidity_specific_path)
        humidity_text = io.StringIO(response["Body"].read().decode())
        humidity = [parse_humidity_record(line.rstrip("\n")) for line in humidity_text.readlines()]

        # # extract images
        # images_name_list = []
        # images_path = os.path.join(client_ID, date, pollination_action, 'realsense')
        # images_path_list = s3.list_objects_v2(Bucket=bucket_name, Prefix=images_path)['Contents']
        #
        # for zip in images_path_list:
        #     zip_name = zip['Key']
        #     response = s3.get_object(Bucket=bucket_name, Key=zip_name)
        #     zip_bytes = io.BytesIO(response['Body'].read())
        #     if is_zipfile(zip_bytes):
        #         zip_file = ZipFile(zip_bytes)
        #         name_list = zip_file.namelist()
        #         extensions = [".jpg", ".png", ".npy"]
        #         for image_name in name_list:
        #             if any(match in image_name for match in extensions):
        #                 images_name_list +=  image_name
        #                 data = zip_file.open(image_name)
        #                 s3.put_object(Body=data, Bucket='bumblebee-bucket3',
        #                               Key=image_name)
        #
        #
        #
        #
        # # rgb_list = rgb_list + [img for img in images if img.startswith('rgb') and img.endswith('.png')]
        # # infrared_list = infrared_list + [img for img in images if img.startswith('infrared')]
        # # depth_list = depth_list + [img for img in images if img.startswith('depth') and img.endswith('.npy')]
        #
        #
        # rgb_list = [name for name in images_name_list if name.startswith('rgb')]
        # infrared_list = [name for name in images_name_list if name.startswith('infrared')]
        # depth_list = [name for name in images_name_list if name.startswith('depth')]
        #
        #
        # images = {'rgb': rgb_list, 'infrared': infrared_list, 'depth': depth_list}

        # mongo_entry = {
        #     'client_ID': client_ID,
        #     'date': date,
        #     'powder_action': pollination_action,
        #     'coordinates': coordinates,
        #     # 'humidity': humidity,
        #     # 'images': images
        # }
        mongo_entry = {
            'coordinates': coordinates,
            'humidity': humidity,
        }
        collection.insert_one(mongo_entry)
        end_time = time.time()
        print('proccess finish! ', end_time - start_time, ' sec')

        return 'Success!'

    except Exception as e:
        print(e)
        raise e



if __name__ == '__main__':
    parse_input('','')
