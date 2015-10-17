__author__ = 'TramAnh'

import xml.sax.handler

NULLChar = "NULL"

class Publication:
    def __init__(self):
        self.pubid = NULLChar
        self.pubkey = NULLChar
        self.title = NULLChar
        self.year = NULLChar

    def toString(self):
        raise NotImplementedError('subclasses must override toString()!')

    def pub_list_for_csv(self):
        return [self.pubid, self.pubkey, self.title, self.year]

    def subclass_list_for_csv(self):
        raise NotImplementedError('subclasses must override toString()!')

class Article(Publication):
    def __init__(self):
        self.journal = NULLChar
        self.month = NULLChar
        self.volume = NULLChar
        self.number = NULLChar

    def toString(self):
        print "Article: "+self.title+"|pubkey:"+self.pubkey+"|year:"+self.year+"|journal:"+self.journal+"|month:"+self.month

    def subclass_list_for_csv(self):
        return [self.pubid, self.journal, self.month, self.volume, self.number]

class Book(Publication):
    def __init__(self):
        self.publisher = NULLChar
        self.isbn = NULLChar

    def toString(self):
        print "Ebook: "+self.title+"|pubkey:"+self.pubkey+"|year:"+self.year+"|publisher:"+self.publisher+"|isbn:"+self.isbn

    def subclass_list_for_csv(self):
        return [self.pubid, self.publisher, self.isbn]

class Incollection(Publication):
    def __init__(self):
        self.booktitle = NULLChar
        self.publisher = NULLChar
        self.isbn = NULLChar

    def toString(self):
        print "Incollection: "+self.title+"|pubkey:"+self.pubkey+"|year:"+self.year+"|publisher:"+self.publisher+"|isbn:"+self.isbn

    def subclass_list_for_csv(self):
        return [self.pubid, self.publisher, self.isbn, self.booktitle]

class Inproceeding(Publication):
    def __init__(self):
        self.booktitle = NULLChar

    def toString(self):
        print "Inproceedings: "+self.title+"|pubkey:"+self.pubkey+"|year:"+self.year+"|booktitle:"+self.booktitle

    def subclass_list_for_csv(self):
        return [self.pubid, self.booktitle]


class PubHandler(xml.sax.handler.ContentHandler):
    pubid = 0
    author = []         # List of authors, id will be the index in list
    authored = []

    def __init__(self):
        self.articles = []
        self.books = []
        self.incollections = []
        self.inproceedings = []

        self.inAuthor, self.inTitle, self.inYear = (0,)*3
        self.inVolume, self.inMonth, self.inJournal, self.inNumber = (0,)*4
        self.inPublisher, self.inIsbn = 0, 0
        self.inBooktitle= 0

    def startElement(self, name, attrs):
        if name == "title": self.inTitle = 1
        elif name == "author": self.inAuthor = 1
        elif name == "year": self.inYear = 1

        elif name == "article":
            # Create Article class
            self.pub = Article()
            self.pub.pubid = PubHandler.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "volume": self.inVolume = 1
        elif name == "journal": self.inJournal = 1
        elif name == "number": self.inNumber = 1
        elif name == "month": self.inMonth = 1

        elif name == "book":
            # Create Book class
            self.pub = Book()
            self.pub.pubid = PubHandler.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "publisher": self.inPublisher = 1
        elif name == "isbn": self.inIsbn = 1
        elif name == "booktitle": self.inBooktitle = 1

        elif name== "incollection":
            # Create Incollection class
            self.pub = Incollection()
            self.pub.pubid = PubHandler.pubid
            self.pub.pubkey = attrs["key"]

        elif name== "inproceedings":
            # Create Inproceeding class
            self.pub = Inproceeding()
            self.pub.pubid = PubHandler.pubid
            self.pub.pubkey = attrs["key"]

    def characters(self, data):
        if self.inAuthor:
            if data not in PubHandler.author:
                # print "Append "+data+" to author"
                PubHandler.author.append(data)
            PubHandler.authored.append((PubHandler.pubid, PubHandler.author.index(data)))
            # print "append authored=(" + str(PubHandler.pubid)+", "+str(PubHandler.author.index(data))+")"

        elif self.inTitle: self.pub.title = data
        elif self.inYear: self.pub.year = data
        elif self.inVolume: self.pub.volume = data
        elif self.inJournal: self.pub.journal = data
        elif self.inMonth: self.pub.month = data
        elif self.inNumber: self.pub.number = data
        elif self.inPublisher: self.pub.publisher = data
        elif self.inIsbn: self.pub.isbn = data
        elif self.inBooktitle: self.pub.booktitle = data

    def endElement(self, name):
        if name=="author": self.inAuthor = 0
        elif name=="title": self.inTitle = 0
        elif name=="year": self.inYear = 0
        elif name=="volume": self.inVolume = 0
        elif name=="journal": self.inJournal = 0
        elif name=="month": self.inMonth = 0
        elif name=="number": self.inNumber = 0
        elif name=="publisher": self.inPublisher = 0
        elif name=="isbn": self.inIsbn = 0
        elif name=="booktitle": self.inBooktitle = 0
        elif name == "article":
            # self.pub.toString()
            self.articles.append(self.pub)
            print "pubid =  "+str(PubHandler.pubid)+" "+name
            PubHandler.pubid += 1

        elif name == "book":
            # self.pub.toString()
            self.books.append(self.pub)
            print "pubid =  "+str(PubHandler.pubid)+" "+name
            PubHandler.pubid += 1

        elif name=="incollection":
            # self.pub.toString()
            self.incollections.append(self.pub)
            print "pubid =  "+str(PubHandler.pubid)+" "+name
            PubHandler.pubid += 1

        elif name=="inproceedings":
            # self.pub.toString()
            self.inproceedings.append(self.pub)
            print "pubid =  "+str(PubHandler.pubid)+" "+name
            PubHandler.pubid += 1

        if(PubHandler.pubid %50000 == 0): print "Processed "+ str(PubHandler.pubid)


def write_to_csv(filename, type):
    with open(filename, 'w') as f:
        writer = csv.writer(f, delimiter=',')

        if type == "article":
            publist = handler.articles
        elif type == "book":
            publist = handler.books

        elif type == "incollection":
            publist = handler.incollections

        # elif type == "inproceedings":
        else:
            publist = handler.inproceedings

        for each in publist:
            writer.writerow(each.subclass_list_for_csv())

def write_publication_authored_author_to_csv():
    # Write author.csv
    with open("author.csv", 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for i in range(0, len(handler.author)):
            writer.writerow(i, handler.author[i])

    # Write authored.csv
    with open("authored.csv", 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for each in handler.authored:
            writer.writerow(each)

    # Write publication.csv
    with open("publication.csv", 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for each_pub_type in [handler.articles, handler.books, handler.incollections, handler.inproceedings]:
            for each_pub in each_pub_type:
                writer.writerow(each_pub.pub_list_for_csv())


import xml.sax
import csv
import time

if __name__ == '__main__':
    start = time.time()
    parser = xml.sax.make_parser()
    handler = PubHandler()
    parser.setContentHandler(handler)
    end_parse = time.time()
    parser.parse('dblp.xml')

    write_to_csv('article.csv', 'article')
    write_to_csv('book.csv', 'book')
    write_to_csv('incollection.csv', 'incolletion')
    write_to_csv('inproceedings.csv', 'inproceedings')
    write_publication_authored_author_to_csv()
    end_writecsv = time.time()

    print "Time to parse: "+ (end_parse - start)
    print "Time to write csv: " +  (end_writecsv - end_parse)

