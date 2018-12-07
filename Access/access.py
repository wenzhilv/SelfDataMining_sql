
import sys
import os
import pypyodbc
import simplelogger
'''
The class is used for serializing the value of the table in access.

pypyodbc introduce:
    connect
    cursor
    cursor.fetchone / fetchall / fetchmany
For example:
ApplyNo Cls  Val Unit
   0001  A    1   g
   0001  B    2   mg
   0001  C    3   ml
   0002  A    11   g
   0002  B    12   mg
   0002  C    13   ml
To
ApplyNo ClsAVal UnitA  ClsBVal UnitB  ClsCVal UnitC
   0001    1      g      2      mg       3      ml
   0002   11      g      12     mg       13     ml
'''

"""
attr:
    __dbconn__ : the connection of Database
    __dbcur__  : the Cursor
"""
SQLBASESELECT= r"SELECT %s FROM %s;"
SQLBASEUPDATE= r"UPDATE ON %s SET %s = %s WHERE %s = %s;"


class Access:
    def __init__(self, dbpath):
        self.__acs_logger = simplelogger.SimpleLogger('acslog', "access").get_simple_logger()
        if os.path.exists(dbpath):
            self.__dbname__ = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=' + dbpath
            self.__dbconn__ = pypyodbc.connect(self.__dbname__)
            self.__dbcur__ = self.__dbconn__.cursor()
        else:
            self.__acs_logger.error("%s is not exists" % dbpath, exc_info=True)
            raise RuntimeError('%s is not exists.' % (dbpath))
        self.__acs_logger.info("access connect ok")

    def __del__(self):
        print("access __del__")
        # self.__acs_logger.info("access  start close")
        if hasattr(self, '__dbconn__'):
            self.__dbcur__.close()
            self.__dbconn__.close()
        # self.__acs_logger.info("access  close ok")
        print('db close over.')

    def sql_excute(self, sqlstatement):
        try:
            self.__dbcur__.execute(sqlstatement)
            pass
            return True
        except:
            self.__acs_logger.error(" error in sql_excute", exc_info=True)
            self.__acs_logger.error(sqlstatement)
            return False

    def sql_select(self, sqlstatement):
        try:
            self.__dbcur__.execute(sqlstatement)
            return True
        except:
            self.__acs_logger.error(" error in sql_select", exc_info=True)
            self.__acs_logger.error(sqlstatement)
            return False

    def sql_cur_fetchone(self):
        try:
            return self.__dbcur__.fetchone()
        except:
            self.__acs_logger.error(" error in sql_cur_fetchone", exc_info=True)
            return False

    def sql_cur_fetchall(self):
        try:
            return self.__dbcur__.fetchall()
        except:
            return False

    def sql_update(self, sqlstatement):
        try:
            self.__dbcur__.execute(sqlstatement)
            self.__dbconn__.commit()
            return True
        except:
            self.__acs_logger.error(" error in sql_update")
            self.__acs_logger.error(sqlstatement)
            return False

    def sql_insert(self, sqlstatement):
        try:
            self.__dbcur__.execute(sqlstatement)
            self.__dbconn__.commit()
            return True
        except:
            return False

    def sql_delete(self, sqlstatement):
        try:
            self.__dbcur__.execute(sqlstatement)
            self.__dbconn__.commit()
            return True
        except:
            return False

    def sql_validation(self, sqlstatement):
        """
        description: validate the sql statement, == sql_excute
        :param sqlstatement: sql statement for validation
        :return: True denotes work out statement, False denotes error.
        """
        try:
            self.__dbcur__.execute(sqlstatement)
            return True
        except:
            return False

    def sql_serial(self, srcInfoTableField, srcResTableField, srcResValField, srcResUnitField, srcResRange):
        pass

    def sql_serial_ptr(self, srcInfoTableField, srcResTableField):
        """
        description: serial a inspection info from srcInfoTableField and srcResTableField to srcInfoFieldCP
        :param srcInfoTableField: string list, the unique apply number.
                    [0] denotes source TABLE name
                    [1] denotes unique number Field name
                    [2] denotes  info cause Field
                    [3] denotes value in info cause Field
        :param srcResTableField: string list, the multiple res.
                    [0] denotes destination TABLE name.
                    [1] denotes destination apply number Field name of the TABLE name.
                    [2] denotes info Field name.
                    [3] denotes value in info Field name
                    [4] denotes result Field name
                    [5] denotes unit Field name.
                    [6] denotes range Field name.
                    [...] denotes other Field name.
        :return:
        """
        pass
        self.__acs_logger.info("start sql_serial_ptr")
        # validate srcinfotable
        if len(srcInfoTableField) < 3 or len(srcResTableField) < 7:
            self.__acs_logger.error("Info or Res Table args len is too short")
            self.__acs_logger.error("Info len is %d, Res len is %d" % (len(srcInfoTableField), len(srcResTableField)))
            raise RuntimeError('Table args len is too short')
        srcTableName = srcInfoTableField[0]
        srcNoFieldName = srcInfoTableField[1]
        srcInfoCauseFieldName = srcInfoTableField[2]
        srcValueInInfoCause = srcInfoTableField[2]
        self._sql_serial_ptr_validationSrcInfoTableField(srcTableName, srcNoFieldName, srcInfoCauseFieldName)


    def _sql_serial_ptr_validationSrcInfoTableField(self, tablename, nofieldname, infofieldname):
        self.__acs_logger.info("start validationSrcInfoTableField")
        sqlstatement = r"SELECT TOP 1  %s, %s FROM %s;" % (nofieldname, infofieldname, tablename)
        if not self.sql_validation(sqlstatement):
            self.__acs_logger.error("sql error in validationSrcInfoTableField", exc_info=True)
            self.__acs_logger.error(sqlstatement)
            raise RuntimeError('validation Error')
            return False
        self.__acs_logger.info("sql validation ok")
        return True


    def get_row_val(self, row, field):
        """
        :param row:
        :param field:
        :return:  row[field]
        """
        if not isinstance(field, str):
            errstr = "field is not string"
            self.__acs_logger.error(errstr)
        field = field.lower()
        try:
            return row[field]
        except:
            self.__acs_logger.error('row[field] is not exists', exc_info=True)
            return False


if __name__ == '__main__':
    pass
    print('access class test')
    # dbpath = r'C:\lwz\softproject\py\data\testprocessxml\Local - process.mdb'
    dbpath = r'C:\Users\202\Desktop\Gynecology Fetal kidney From Prof Zeng\Intermediate results\Local - new process.mdb'
    objacs = Access(dbpath)
    sqlselect = "SELECT t.PatientId, t.VisitId, t.LsContent FROM PatientDischargeSummary AS t;"
    if not objacs.sql_excute(sqlselect):
        errstr = "sql_excute %s" % sqlselect
        raise RuntimeError(errstr)
    tcnt = 0
    while True:
        row = objacs.sql_cur_fetchone()
        if not row or tcnt > 5:
            break
        tcnt += 1
        # pid = row['PatientId'] #error it is row['patientid']
        # vid = row['VisitId']
        pid = row['patientid']
        vid = row['visitid']
        print(pid)
        print(vid)
