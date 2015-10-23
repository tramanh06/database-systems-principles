__author__ = 'TramAnh'

import StringIO
import xml.sax
from xml.sax.handler import ContentHandler

# Inheriting from EntityResolver and DTDHandler is not necessary
class TestHandler(ContentHandler):

    # This method is only called for external entities. Must return a value.
    def resolveEntity(self, publicID, systemID):
        print "TestHandler.resolveEntity(): %s %s" % (publicID, systemID)
        return systemID

    def skippedEntity(self, name):
        print "TestHandler.skippedEntity(): %s" % (name)

    def unparsedEntityDecl(self, name, publicID, systemID, ndata):
        print "TestHandler.unparsedEntityDecl(): %s %s" % (publicID, systemID)

    def startElement(self, name, attrs):
        summary = attrs.get('summary', '')
        print 'TestHandler.startElement():', summary
        if name == "test":
            self.inTest = 1

    def characters(self, content):
        if self.inTest:
            print "content="+ content

def main(xml_string):
    try:
        parser = xml.sax.make_parser()
        curHandler = TestHandler()
        parser.setContentHandler(curHandler)
        parser.setEntityResolver(curHandler)
        parser.setDTDHandler(curHandler)

        stream = StringIO.StringIO(xml_string)
        parser.parse(stream)
        stream.close()
    except xml.sax.SAXParseException, e:
        print "*** PARSER error: %s" % e

XML = """<!DOCTYPE test SYSTEM "test.dtd">
<test summary='step: &num;'>Entity: &not;</test>
"""

main(XML)
