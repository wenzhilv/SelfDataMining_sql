
import logging
import time
import os


class SimpleLogger:
    def __init__(self, logfilename, loggername):
        logfilebase = r".\log"
        if not os.path.isdir(logfilebase):
            os.mkdir(logfilebase)
        logtime = logfilename + time.strftime("%Y%m%d%H%M%S", time.localtime(time.time())) + r'.log'
        logfile = os.path.join(logfilebase, logtime)
        LOG_FORMAT = "%(levelname)s - %(name)s -  %(message)s"
        formatter = logging.Formatter(LOG_FORMAT)
        self.__logger = logging.getLogger(loggername)
        self.__logger.setLevel(logging.INFO)
        handler = logging.FileHandler(logfile)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)
        # console = logging.StreamHandler()
        # console.setLevel(logging.INFO)
        self.__logger.addHandler(handler)
        self.__logger.info("logging __init__")

    def __del__(self):
        self.__logger.info("logging __del__")

    def get_simple_logger(self):
        self.__logger.info("logging get_simple_logger")
        return self.__logger

