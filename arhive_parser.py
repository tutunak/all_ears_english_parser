import sys
import datetime
import argparse
import boto3


from AEE import AllEarsEnglishArchiveParser


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='File to parse')
    return parser.parse_args()


def table_put(table, items, index):
    print('Writing Podcast Episode: {}'.format(items[index]))
    table.put_item(
        Item={
            'href': items[index]['href'],
            'title': items[index]['title'],
            'timestamp': int(datetime.datetime.now().timestamp() * 1000000),
        }
    )


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
    ars = parse_args()
    archive = AllEarsEnglishArchiveParser(ars.file)
    dynamodb_write(archive)


if __name__ == '__main__':
    main()
