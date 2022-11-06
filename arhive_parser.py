import sys
import argparse

from bs4 import BeautifulSoup

from AEE import AllEarsEnglishArchiveParser


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='File to parse')
    return parser.parse_args()


def main():
    ars = parse_args()
    archive = AllEarsEnglishArchiveParser(ars.file)
    print(archive.items.general_fluency)
    print(archive.items.ielts)


if __name__ == '__main__':
    main()
