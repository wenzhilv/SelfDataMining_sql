
import traceback


class SqlAccess:
    def __init__(self):
        pass

    def __del__(self):
        pass

    def get_sql_creat_table(self, fieldsname, fieldsstrlen):
        """
        :param fieldsname:
        :param fieldsstrlen: TEXT(fieldsstrlen)
        :return: sel statement
        """
        if len(fieldsname) != len(fieldsstrlen):
            errstr = "len(fieldsname) != len(fieldsstrlen) in get_sql_creat_table"
            print(errstr)
            return False
        sqlfields = ""
        for index in range(len(fieldsname)):
            sqlfields = sqlfields + (" %s TEXT(%d), " % (fieldsname[index], fieldsstrlen[index]))
        sqlfields = sqlfields[:-2]
        resql = " CREATE TABLE tblCustomers %s " % sqlfields
        return resql

    def get_sql_add_field(self, tablename, fieldname, fieldstrlen):
        resql = " ALTER TABLE %s ADD COLUMN %s TEXT(%d) " % (tablename, fieldname, fieldstrlen)
        return resql

    def get_sql_drop_field(self, tablename, fieldname):
        resql = " ALTER TABLE %s DROP  COLUMN %s " % (tablename, fieldname)
        return resql

    def get_sql_alter_field(self, tablename, fieldname, fieldstrlen):
        resql = " ALTER TABLE %s ALTER  COLUMN %s TEXT(%d) " % (tablename, fieldname, fieldstrlen)
        return resql

    def get_sql_insert_one(self, tablename, fieldsname, fieldsval):
        """
        :param tablename:
        :param fieldsname:
        :param fieldsval:
        :return:
        """
        if len(fieldsname) != len(fieldsval):
            errstr = "len(fieldsname) != len(fieldsval) in get_sql_insert_one"
            print(errstr)
            return False
        sqlfield = ""
        sqlval = ""
        for index in range(len(fieldsname)):
            sqlfield = sqlfield + ("%s, " % fieldsname[index])
            str = fieldsval[index]
            str = str.replace("\'", "\"")
            sqlval = sqlval + ("\'%s\', " % str)
        sqlfield = sqlfield[:-2]
        sqlval = sqlval[:-2]
        resql = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, sqlfield, sqlval)
        return resql

    def get_sql_insert_all(self, srctable, srcfields, desttable, destfields):
        if len(srcfields) != len(destfields):
            errstr = "len(fieldsname) != len(fieldsval) in get_sql_insert_one"
            print(errstr)
            return False
        sqlsrc = ""
        sqldest = ""
        for index in range(len(srcfields)):
            sqlsrc = sqlsrc + ("[%s], " % srcfields[index])
            sqldest = sqldest + ("[%s], " % destfields[index])
        sqlsrc = sqlsrc[:-2]
        sqldest = sqldest[:-2]
        resql = " INSERT INTO %s (%s) SELECT %s FROM %s " % (srctable, sqlsrc, desttable, sqldest)
        return resql

    def get_sql_select(self, fieldnames, tablename):
        """
        :param fieldnames:
        :param tablename:
        :return:  SELECT ...fieldnames FROM tablename;
        """
        if fieldnames is None or tablename is None:
            errstr = "fieldnames is %s, tablename is %s. have none" % (fieldnames, tablename)
            # self.__acs_logger.error(errstr)
            return False
        if not isinstance(tablename, str):
            errstr = "tablename is not string"
            # self.__acs_logger.error(errstr)
            return False
        sqlfrom = " FROM (%s) AS t " % tablename
        sqlselect = ""
        for tn in fieldnames:
            if not isinstance(tn, str):
                errstr = "tn is not string"
                # self.__acs_logger.error(errstr)
                return False
            sqlselect = sqlselect + ("t.%s, " % tn)
        sqlselect = sqlselect[:-2]
        sqlstatement = "SELECT " + sqlselect + sqlfrom
        return sqlstatement

    def get_sql_innerjoin(self, fieldnames, fromtable, jointable, formfields, joinfields):
        if fieldnames is None or fromtable is None or jointable is None or formfields is None or joinfields is None:
            errstr = "argus in get_sql_innerjoin have None"
            print(errstr)
            # self.__acs_logger.error(errstr)
            return False
        if len(formfields) != len(joinfields):
            errstr = "len(formfields) != len(joinfields)"
            print(errstr)
            # self.__acs_logger.error(errstr)
            return False
        sqlsel = self.get_sql_select(fieldnames, fromtable)
        sqlinnerbase = " INNER JOIN %s ON (%s)"
        sqljoinbase = "(%s) AS m " % jointable
        sqlonbase = ""
        for index in range(len(formfields)):
            sqlonbase += ("t.%s = m.%s AND " % (formfields[index], joinfields[index]))
        sqlonbase = sqlonbase[:-4]
        sqlinner = sqlinnerbase % (sqljoinbase, sqlonbase)
        sqlstatement = sqlsel + sqlinner
        return sqlstatement

    def get_sql_innerjoin_str(self, selectstatement, joinstable, joinonstr):
        resql = " %s INNER JOIN (%s) AS m ON (%s)" % (selectstatement, joinstable, joinonstr)
        return resql

    def get_sql_mindatetime(self, showfields, table, timefield):
        sqljoin = " SELECT jt.PatientId, (max(CDate(jt.%s))) AS maxDateTime FROM %s AS jt GROUP BY jt.PatientId " % (timefield, table)
        sqlsel = self.get_sql_select(showfields, table)
        sqlon = " t.PatientId = m.PatientId AND CDATE(t.%s) = m.maxDateTime " % timefield
        resql = self.get_sql_innerjoin_str(sqlsel, sqljoin, sqlon)
        return resql


if __name__ == '__main__':
    print("test sqlaccess")
    showfields = ['patientid', 'name', 'examdatetime']
    table = 'PatientInfo'
    timefield = 'examdatetime'
    objsql = SqlAccess()
    print(objsql.get_sql_mindatetime(showfields, table, timefield))
