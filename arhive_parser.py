import os
import sys
import datetime
import argparse
import time
import random

import boto3
import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup


from AEE import AllEarsEnglishArchiveParser


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='File to parse')
    return parser.parse_args()


def table_put(table, items, index):
    item = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('href').eq(items[index]['href'])
        )

    if item['Items']:
        print(f"Item already exists{item}")
        return

    print('Writing Podcast Episode: {}'.format(items[index]))
    table.put_item(
        Item={
            'href': items[index]['href'],
            'title': items[index]['title'],
            'timestamp': int(datetime.datetime.now().timestamp() * 1000000),
            'state': 0
        }
    )


def mongodb_put(collection, reversed_items, podcast_episode):
    item = collection.find_one({'href': reversed_items[podcast_episode]['href']})
    if item:
        print(f"Item already exists{item}")
        return

    print('Writing Podcast Episode: {}'.format(reversed_items[podcast_episode]))
    collection.insert_one({
        'href': reversed_items[podcast_episode]['href'],
        'title': reversed_items[podcast_episode]['title'],
        'timestamp': int(datetime.datetime.now().timestamp() * 1000000),
        'state': 0
    })

# def drop_index(collection):
#     collection.drop_index([('timestamp', -1), ('state', 1), ('href', 1)])

def grab_an_href(collection):
    item = collection.find_one({'state': 0})
    if item:
        print(f"Item already exists{item}")
        return item
    else:
        print("No items found")
        return None

def get_page(item):
    response = requests.get(item['href'])
    if response.status_code == 200:
        return response.text
    else:
        print("Failed to get page")
        return None

def save_page_to_collection(collection, item, page):
    collection.update_one(
        {'href': item['href']},
        {'$set': {'page': page, 'state': 1, 'published_in_telegram': 0}}
    )
def mongodb_write(client, archive):
    db = client['AEE']
    collection = db['AEE']
    reversed_general_fluency = archive.items.general_fluency[::-1]
    reversed_ielts = archive.items.ielts[::-1]
    general_fluency_count = len(archive.items.general_fluency)
    ielts_count = len(archive.items.ielts)
    if general_fluency_count > ielts_count:
        counter = general_fluency_count
    else:
        counter = ielts_count
    for podcast_episode in range(counter):
        print("Processing podcast episode: {} from {}".format(podcast_episode, counter))
        if not (podcast_episode >= general_fluency_count):
            mongodb_put(collection, reversed_general_fluency, podcast_episode)

        if not (podcast_episode >= ielts_count):
            mongodb_put(collection, reversed_ielts, podcast_episode)
def dynamodb_write(archive):
    database = boto3.resource('dynamodb')
    table = database.Table('AEE')
    reversed_general_fluency = archive.items.general_fluency[::-1]
    reversed_ielts = archive.items.ielts[::-1]
    general_fluency_count = len(archive.items.general_fluency)
    ielts_count = len(archive.items.ielts)
    if general_fluency_count > ielts_count:
        counter = general_fluency_count
    else:
        counter = ielts_count
    for podcast_episode in range(counter):
        print("Processing podcast episode: {} from {}".format(podcast_episode, counter))
        if not (podcast_episode >= general_fluency_count):
            table_put(table, reversed_general_fluency, podcast_episode)

        if not (podcast_episode >= ielts_count):
            table_put(table, reversed_ielts, podcast_episode)

def main():
    connection_string = os.getenv('MONGO_CONNECTION_STRING')
    if not connection_string:
        print("MONGO_CONNECTION_STRING is not set")
        sys.exit(1)
    client = MongoClient(connection_string)



    ars = parse_args()
    archive = AllEarsEnglishArchiveParser(ars.file)
    # mongodb_write(client, archive)
    # dynamodb_write(archive)
    db = client['AEE']
    collection = db['AEE']
    while True:
        item = grab_an_href(collection)
        if item:
            page = get_page(item)
            if page:
                save_page_to_collection(collection, item, page)
        time.sleep(random.randint(1, 5))


if __name__ == '__main__':
    main()
