__author__ = 'TramAnh'

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
