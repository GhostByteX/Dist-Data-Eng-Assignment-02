import boto3
import ast

# Define the AWS region and DynamoDB table name
region_name = 'ap-northeast-1'
table_name = 'my-bitcoin-tweets'

# Create a DynamoDB resource
session = boto3.Session(
    aws_access_key_id='AKIA5VWRR5KMRTHQQITU',
    aws_secret_access_key='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'
)
dynamodb = session.resource('dynamodb', region_name = region_name)
# Select the table
table = dynamodb.Table(table_name)

# Get all tweets of a user
def get_tweets_by_user(username):
    response = table.query(
        KeyConditionExpression='username = :u',
        ExpressionAttributeValues={
            ':u': username
        }
    )
    return response['Items']


def get_tweets_by_location(my_location):
    response = table.scan(
        IndexName='userlocation-index',
        FilterExpression='userlocation = :l',
        ExpressionAttributeValues={
            ':l': my_location
        }
    )
    return response['Items']


def get_top_users(k):
    response = table.scan(
        ProjectionExpression='username, followers',
        Limit=k,
        ReturnConsumedCapacity='TOTAL'
    )
    users = sorted(response['Items'], key=lambda u: u['followers'], reverse=True)
    return users[:k]



def get_tweets_by_top_users(k):
    users = get_top_users(k)
    tweets = []
    for user in users:
        username = user['username']
        response = table.query(
            KeyConditionExpression='username = :u',
            ExpressionAttributeValues={
                ':u': username
            }
        )
        tweets.extend(response['Items'])
    return tweets

# Get top k tweets with the most matching tags
def get_top_tweets_by_tags(k, tags):
    response = table.scan(
        FilterExpression='contains(hashtags, :tags)',
        ExpressionAttributeValues={
            ':tags': tags
        },
        ProjectionExpression='hashtags, favorites',
        Limit=k,
        ReturnConsumedCapacity='TOTAL'
    )
    tweets = sorted(response['Items'], key=lambda t: len(set(ast.literal_eval(t['hashtags'])) & set(tags)), reverse=True)
    return tweets[:k]


# Delete all posts of user with followers less than a threshold
def delete_tweets_by_followers(threshold):
    response = table.scan(
        ProjectionExpression='username, followers',
        FilterExpression='followers < :f',
        ExpressionAttributeValues={
            ':f': threshold
        },
        ReturnConsumedCapacity='TOTAL'
    )
    for item in response['Items']:
        username = item['username']
        table.delete_item(
            Key={
                'username': username,
                'date': item['date']
            }
        )
    

# Test the functions
if __name__ == '__main__':
    print("\n\nGET TWEETS BY USER:\n\n",get_tweets_by_user('Mike Chambers at Northey Point'))
    print("\n\nGET TWEETS BY SAME USERS IN A LOCATION:\n\n",get_tweets_by_location('England, United Kingdom'))
    print("\n\nGET TOP k USERS:\n\n",get_top_users(3))
    print("\n\nGET TWEETS by TOP k USERS:\n\n",get_tweets_by_top_users(3))
    print("\n\nGET TOP k TWEETS WITH MACTHING TAGS:\n\n",get_top_tweets_by_tags(3,['blockchain','bitcoin']))
    delete_tweets_by_followers(1000)
