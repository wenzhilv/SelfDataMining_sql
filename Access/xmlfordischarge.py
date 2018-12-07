
import xml.etree.ElementTree as ET
import os
import shutil
import simplelogger

# xmllogger = simplelogger.SimpleLogger('xmllog', 'access').get_simple_logger()

class XmlParseString:
    def __init__(self, filepathOrString):
        # self.__logger = simplelogger.SimpleLogger('xmllog', logger=access').getlog()
        if os.path.isfile(filepathOrString):
            filename = os.path.abspath(filepathOrString)
            self.__root = ET.parse(filename).getroot()
        else:
            self.__root = ET.fromstring(filepathOrString)
        # xmllogger.info("xml root create OK")

    def __del__(self):
        # xmllogger.info("__del__ XmlParseString")
        pass

    def getFirstTagString(self, tagName):
        try:
            return self.__root.find(tagName).text
        except:
            # xmllogger.error(" element %s is not in the xml" % tagName)
            raise RuntimeError('Tags is not in the file.')


if __name__ == '__main__':
    print("test some thing")
