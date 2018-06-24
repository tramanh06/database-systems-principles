__author__ = 'TramAnh'

import xml.sax.handler
from xml.sax.handler import ContentHandler,EntityResolver
import pub_data
import xml.sax
import csv
import time
import logging

logging.basicConfig(filename='pubhandler.log',level=logging.DEBUG)
delimiter_char = '\t'

class PubHandler(ContentHandler):

    def __init__(self):

        self.inAuthor, self.inTitle, self.inYear = (0,)*3
        self.inVolume, self.inMonth, self.inJournal, self.inNumber = (0,)*4
        self.inPublisher, self.inIsbn = 0, 0
        self.inBooktitle= 0

        self.pubid = 0

        a_file = open("./Data/article.csv", 'w')
        self.article_writer = csv.writer(a_file, delimiter=delimiter_char)

        b_file = open("./Data/book.csv", 'w')
        self.book_writer = csv.writer(b_file, delimiter=delimiter_char)

        c_file = open("./Data/incollection.csv", 'w')
        self.incollection_writer = csv.writer(c_file, delimiter=delimiter_char)

        p_file = open("./Data/inproceedings.csv", 'w')
        self.inproceedings_writer = csv.writer(p_file, delimiter=delimiter_char)

        pu_file = open("./Data/publication.csv", 'w')
        self.publication_writer = csv.writer(pu_file, delimiter=delimiter_char)

        au_file = open("./Data/authored.csv", 'w')
        self.authored_writer = csv.writer(au_file, delimiter=delimiter_char)

        # author_file = open("./Data/author.csv", 'w')
        # self.author_writer = csv.writer(author_file, delimiter=delimiter_char)


    def resolveEntity(self,publicID,systemID):
        print "TestHandler.resolveEntity: %s  %s" % (publicID,systemID)
        return systemID
    #
    # def skippedEntity(self, name):
    #     print "TestHandler.skippedEntity(): %s" % (name)
    #
    # def unparsedEntityDecl(self, name, publicID, systemID, ndata):
    #     print "TestHandler.unparsedEntityDecl(): %s %s" % (publicID, systemID)


    def startElement(self, name, attrs):
        if name == "title":
            self.inTitle = 1
            self.buffer=''
        elif name == "author":
            self.inAuthor = 1
            self.buffer = ''
        elif name == "year": self.inYear = 1

        elif name == "article":
            # Create Article class
            self.pub = pub_data.Article()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "volume": self.inVolume = 1
        elif name == "journal":
            self.inJournal = 1
            self.buffer = ''
        elif name == "number": self.inNumber = 1
        elif name == "month": self.inMonth = 1

        elif name == "book":
            # Create Book class
            self.pub = pub_data.Book()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "publisher":
            self.inPublisher = 1
            self.buffer = ''
        elif name == "isbn": self.inIsbn = 1
        elif name == "booktitle":
            self.inBooktitle = 1
            self.buffer = ''

        elif name== "incollection":
            # Create Incollection class
            self.pub = pub_data.Incollection()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name== "inproceedings":
            # Create Inproceeding class
            self.pub = pub_data.Inproceeding()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

    def characters(self, data):
        if self.inAuthor: self.buffer += data
        elif self.inTitle: self.buffer += data
        elif self.inYear: self.pub.year = data
        elif self.inVolume: self.pub.volume = data
        elif self.inJournal: self.buffer += data
        elif self.inMonth: self.pub.month = data
        elif self.inNumber: self.pub.number = data
        elif self.inPublisher: self.buffer += data
        elif self.inIsbn: self.pub.isbn = data
        elif self.inBooktitle: self.buffer += data



    def endElement(self, name):
        if name=="author":
            self.inAuthor = 0
            # Write to authored.csv
            self.authored_writer.writerow((self.pubid, unicode(self.buffer).encode("utf-8")))
            self.buffer = ''

        elif name=="title":
            self.pub.title = unicode(self.buffer).encode("utf-8")
            self.buffer = ''
            self.inTitle = 0

        elif name=="year": self.inYear = 0
        elif name=="volume": self.inVolume = 0
        elif name=="journal":
            self.pub.journal = unicode(self.buffer).encode("utf-8")
            self.buffer = ''
            self.inJournal = 0
        elif name=="month": self.inMonth = 0
        elif name=="number": self.inNumber = 0
        elif name=="publisher":
            self.inPublisher = 0
            self.pub.publisher = unicode(self.buffer).encode("utf-8")
            self.buffer = ''

        elif name=="isbn": self.inIsbn = 0
        elif name=="booktitle":
            self.inBooktitle = 0
            self.pub.booktitle = unicode(self.buffer).encode("utf-8")
            self.buffer = ''

        elif name == "article":
            self.article_writer.writerow(self.pub.subclass_list_for_csv())

        elif name == "book":
            self.book_writer.writerow(self.pub.subclass_list_for_csv())

        elif name=="incollection":
            self.incollection_writer.writerow(self.pub.subclass_list_for_csv())

        elif name=="inproceedings":
            self.inproceedings_writer.writerow(self.pub.subclass_list_for_csv())

        if name=="article" or name=="book" or name=="incollection" or name=="inproceedings":
            self.pubid += 1
            self.publication_writer.writerow(self.pub.pub_list_for_csv())
            if(self.pubid %100000 == 0):
                print "pubid= "+ str(self.pubid)+" "+name


if __name__ == '__main__':
    start = time.time()

    parser = xml.sax.make_parser()
    handler = PubHandler()
    parser.setContentHandler(handler)
    parser.setEntityResolver(handler)

    parser.parse('dblp.xml')
    end_parse = time.time()

    print "Time to parse: "+ str(end_parse - start)
    logging.info("Finished parsing: "+ str(handler.pubid) +" pub items.")
    logging.info("Time to parse: "+ str(end_parse - start))




