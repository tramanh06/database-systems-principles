__author__ = 'TramAnh'

import xml.sax.handler

NULLChar = "\N"

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
        Publication.__init__(self)
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

    # authored = []

    def __init__(self):

        self.inAuthor, self.inTitle, self.inYear = (0,)*3
        self.inVolume, self.inMonth, self.inJournal, self.inNumber = (0,)*4
        self.inPublisher, self.inIsbn = 0, 0
        self.inBooktitle= 0

        self.pubid = 0
        self.author = []         # List of authors, id will be the index in list

        a_file = open("article.csv", 'w')
        self.article_writer = csv.writer(a_file, delimiter=',')

        b_file = open("book.csv", 'w')
        self.book_writer = csv.writer(b_file, delimiter=',')

        c_file = open("incollection.csv", 'w')
        self.incollection_writer = csv.writer(c_file, delimiter=',')

        p_file = open("inproceedings.csv", 'w')
        self.inproceedings_writer = csv.writer(p_file, delimiter=',')

        pu_file = open("publication.csv", 'w')
        self.publication_writer = csv.writer(pu_file, delimiter=',')

        au_file = open("authored.csv", 'w')
        self.authored_writer = csv.writer(au_file, delimiter=',')

    def startElement(self, name, attrs):
        if name == "title": self.inTitle = 1
        elif name == "author": self.inAuthor = 1
        elif name == "year": self.inYear = 1

        elif name == "article":
            # Create Article class
            self.pub = Article()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "volume": self.inVolume = 1
        elif name == "journal": self.inJournal = 1
        elif name == "number": self.inNumber = 1
        elif name == "month": self.inMonth = 1

        elif name == "book":
            # Create Book class
            self.pub = Book()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "publisher": self.inPublisher = 1
        elif name == "isbn": self.inIsbn = 1
        elif name == "booktitle": self.inBooktitle = 1

        elif name== "incollection":
            # Create Incollection class
            self.pub = Incollection()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name== "inproceedings":
            # Create Inproceeding class
            self.pub = Inproceeding()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

    def characters(self, data):
        if self.inAuthor:
            if data not in self.author:
                # print "Append "+data+" to author"
                self.author.append(data)
            # PubHandler.authored.append((PubHandler.pubid, PubHandler.author.index(data)))
            self.authored_writer.writerow((self.pubid, self.author.index(data)))
            # print "append authored=(" + str(PubHandler.pubid)+", "+str(PubHandler.author.index(data))+")"

        elif self.inTitle: self.pub.title = data
        elif self.inYear:
            self.pub.year = data
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
            print "pubid = "+str(self.pubid)+" "+name
            self.article_writer.writerow(self.pub.subclass_list_for_csv())


        elif name == "book":
            print "pubid = "+str(self.pubid)+" "+name
            self.book_writer.writerow(self.pub.subclass_list_for_csv())

        elif name=="incollection":
            print "pubid = "+str(self.pubid)+" "+name
            self.incollection_writer.writerow(self.pub.subclass_list_for_csv())

        elif name=="inproceedings":
            print "pubid = "+str(self.pubid)+" "+name
            self.inproceedings_writer.writerow(self.pub.subclass_list_for_csv())

        if name=="article" or name=="book" or name=="incollection" or name=="inproceedings":
            self.pubid += 1
            self.publication_writer.writerow(self.pub.pub_list_for_csv())


        if(self.pubid %50000 == 0): print "Processed "+ str(self.pubid)



import xml.sax
import csv
import time

def close_writers():
    handler.authored_writer.close()
    handler.publication_writer.close()
    handler.inproceedings_writer.close()
    handler.incollection_writer.close()
    handler.book_writer.close()
    handler.article_writer.close()

if __name__ == '__main__':
    start = time.time()
    parser = xml.sax.make_parser()
    handler = PubHandler()
    parser.setContentHandler(handler)
    parser.parse('dblp.xml')
    end_parse = time.time()


    print "Time to parse: "+ (end_parse - start)

    close_writers()
    print "Writin author.csv..."
    with open("author.csv", 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for i in range(0, len(handler.author)):
            writer.writerow(i, handler.author[i])
    print "Time to write author.csv: "+ (time.time() - end_parse)


