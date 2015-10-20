__author__ = 'TramAnh'

import xml.sax.handler
from xml.sax.handler import ContentHandler,EntityResolver
import pub_data
import xml.sax
import csv
import time
import sys
import codecs


class PubHandler(ContentHandler):

    def __init__(self):

        self.inAuthor, self.inTitle, self.inYear = (0,)*3
        self.inVolume, self.inMonth, self.inJournal, self.inNumber = (0,)*4
        self.inPublisher, self.inIsbn = 0, 0
        self.inBooktitle= 0

        self.pubid = 0
        # self.author = []         # List of authors, id will be the index in list

        a_file = open("./Data/article.csv", 'w')
        self.article_writer = csv.writer(a_file, delimiter=',')

        b_file = open("./Data/book.csv", 'w')
        self.book_writer = csv.writer(b_file, delimiter=',')

        c_file = open("./Data/incollection.csv", 'w')
        self.incollection_writer = csv.writer(c_file, delimiter=',')

        p_file = open("./Data/inproceedings.csv", 'w')
        self.inproceedings_writer = csv.writer(p_file, delimiter=',')

        pu_file = open("./Data/publication.csv", 'w')
        self.publication_writer = csv.writer(pu_file, delimiter=',')

        au_file = open("./Data/authored.csv", 'w')
        self.authored_writer = csv.writer(au_file, delimiter=',')

        author_file = open("./Data/author.csv", 'w')
        self.author_writer = csv.writer(author_file, delimiter=',')


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
            self.title_buffer=''
        elif name == "author":
            self.inAuthor = 1
            self.author_buffer = ''
        elif name == "year": self.inYear = 1

        elif name == "article":
            # Create Article class
            self.pub = pub_data.Article()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "volume": self.inVolume = 1
        elif name == "journal": self.inJournal = 1
        elif name == "number": self.inNumber = 1
        elif name == "month": self.inMonth = 1

        elif name == "book":
            # Create Book class
            self.pub = pub_data.Book()
            self.pub.pubid = self.pubid
            self.pub.pubkey = attrs["key"]

        elif name == "publisher":
            self.inPublisher = 1
            self.publisher_buffer = ''
        elif name == "isbn": self.inIsbn = 1
        elif name == "booktitle":
            self.inBooktitle = 1
            self.booktitle_buffer = ''

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
        if self.inAuthor: self.author_buffer += data
        elif self.inTitle: self.title_buffer += data
        elif self.inYear: self.pub.year = data
        elif self.inVolume: self.pub.volume = data
        elif self.inJournal: self.pub.journal = data
        elif self.inMonth: self.pub.month = data
        elif self.inNumber: self.pub.number = data
        elif self.inPublisher: self.publisher_buffer += data
        elif self.inIsbn: self.pub.isbn = data
        elif self.inBooktitle: self.booktitle_buffer += data

    def endElement(self, name):
        if name=="author":
            self.inAuthor = 0
            # Check if author is in database
            # if self.author_buffer not in self.author:
            #     self.author.append(self.author_buffer)
            #     author_index = self.author.index(self.author_buffer)
            #
            #     # Write to author.csv
            #     self.author_writer.writerow([author_index, unicode(self.author_buffer).encode("utf-8")])
            # else:
            #     author_index = self.author.index(self.author_buffer)

            # Write to authored.csv
            self.authored_writer.writerow((self.pubid, unicode(self.author_buffer).encode("utf-8")))
            self.author_buffer = ''

        elif name=="title":
            self.pub.title = unicode(self.title_buffer).encode("utf-8")
            self.title_buffer = ''
            self.inTitle = 0
        elif name=="year": self.inYear = 0
        elif name=="volume": self.inVolume = 0
        elif name=="journal": self.inJournal = 0
        elif name=="month": self.inMonth = 0
        elif name=="number": self.inNumber = 0
        elif name=="publisher":
            self.inPublisher = 0
            self.pub.publisher = unicode(self.publisher_buffer).encode("utf-8")
            self.publisher_buffer = ''

        elif name=="isbn": self.inIsbn = 0
        elif name=="booktitle":
            self.inBooktitle = 0
            self.pub.booktitle = unicode(self.booktitle_buffer).encode("utf-8")
            self.booktitle_buffer = ''

        elif name == "article":
            # print "pubid = "+str(self.pubid)+" "+name
            self.article_writer.writerow(self.pub.subclass_list_for_csv())

        elif name == "book":
            # print "pubid = "+str(self.pubid)+" "+name
            self.book_writer.writerow(self.pub.subclass_list_for_csv())

        elif name=="incollection":
            # print "pubid = "+str(self.pubid)+" "+name
            self.incollection_writer.writerow(self.pub.subclass_list_for_csv())

        elif name=="inproceedings":
            # print "pubid = "+str(self.pubid)+" "+name
            self.inproceedings_writer.writerow(self.pub.subclass_list_for_csv())

        if name=="article" or name=="book" or name=="incollection" or name=="inproceedings":
            self.pubid += 1
            self.publication_writer.writerow(self.pub.pub_list_for_csv())
            if(self.pubid %100 == 0):
                print "pubid= "+ str(self.pubid)+" "+name




def close_writers():
    handler.authored_writer.close()
    handler.publication_writer.close()
    handler.inproceedings_writer.close()
    handler.incollection_writer.close()
    handler.book_writer.close()
    handler.article_writer.close()
    handler.author_writer.close()

if __name__ == '__main__':
    start = time.time()

    parser = xml.sax.make_parser()
    handler = PubHandler()
    parser.setContentHandler(handler)
    parser.setEntityResolver(handler)

    parser.parse('dblp.xml')
    end_parse = time.time()

    print "Time to parse: "+ str(end_parse - start)

    close_writers()



