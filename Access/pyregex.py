
"""
Class for regex in python
"""

import re


class Regex:
    def __int__(self):
        self.__regex = ""
        self.__pattern = ""
        self.__reflag = re.I|re.M

    def __del__(self):
        print("regex over")

    def set_regex(self, restr, flag=re.I|re.M):
        self.__regex = restr
        self.__reflag = flag
        self.__pattern = re.compile(self.__regex)

    def get_res_first(self, instr):
        res = self.__pattern.search(instr)
        return res

    def get_res_all(self, instr):
        res = self.__pattern.findall(instr)
        return res
