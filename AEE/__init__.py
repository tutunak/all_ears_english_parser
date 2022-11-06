from bs4 import BeautifulSoup

class ArchivedItems:
    def __init__(self, soup):
        self.soup = soup
        self.__general_fluency = None
        self.__ielts = None

    def get_items(self):
        return self.soup.find_all('item')

    @property
    def general_fluency(self):
        if not self.__general_fluency:
            self.__general_fluency = self.soup.find('div', {'class': 'custom-archive-items'})
        return self.__general_fluency.find_all('a')

    @property
    def ielts(self):
        if not self.__ielts:
            self.__ielts = self.soup.find('div', {'id': 'ielts-archive'})
        return self.__ielts.find_all('a')


class AllEarsEnglishArchiveParser:
    def __init__(self, filename):
        self.filename = filename
        self.__html = None
        self.__soup = None
        self.__custom_archive = None
        self.__ielts_archive = None
        self.__items = None

    @property
    def html(self):
        if not self.__html:
            with open(self.filename, 'r') as f:
                self.__html = f.read()
        return self.__html

    @property
    def soup(self):
        if not self.__soup:
            self.__soup = BeautifulSoup(self.html, 'html.parser')
        return self.__soup

    @property
    def items(self):
        if not self.__items:
            self.__items = ArchivedItems(self.soup)
        return self.__items
