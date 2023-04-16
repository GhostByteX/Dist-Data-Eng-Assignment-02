import boto3
import csv
from datetime import datetime
from decimal import Decimal
import config

# Create a DynamoDB client
region_name = 'ap-northeast-1'
table_name = 'my-bitcoin-tweets'

# Create a DynamoDB resource
session = boto3.Session(
    aws_access_key_id=config.aws_access_key_id,
    aws_secret_access_key=config.aws_secret_access_key
)
dynamodb = session.resource('dynamodb', region_name = region_name)
table = dynamodb.Table(table_name)

# Open the CSV file and read the data into a dictionary
with open('Bitcoin_tweets.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Convert the date value to a datetime object
        date_str = row['date']
        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

        # Convert the user_created value to a datetime object
        created_str = row['user_created']
        created_obj = datetime.strptime(created_str, '%Y-%m-%d %H:%M:%S')

        # Convert the user_followers value to a Decimal type
        followers = Decimal(str(row['user_followers']))

        # Create a new item to add to the table
        item = {
            'username': row['user_name'],
            'userlocation': row['user_location'],
            'description': row['user_description'],
            'created': created_obj.isoformat(),
            'followers': str(followers),
            'friends': row['user_friends'],
            'favorites': row['user_favourites'],
            'verified': row['user_verified'].lower() == 'true',
            'date': date_obj.isoformat(),
            'text': row['text'],
            'hashtags': row['hashtags'],
            'source': row['source'],
            'is_retweet': row['is_retweet'].lower() == 'true'
        }
        
        item = {k: v for k, v in item.items() if v}
        response = table.put_item(Item=item)