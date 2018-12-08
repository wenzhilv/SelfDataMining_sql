
import traceback


class SqlAccess:
    def __init__(self):
        pass

    def __del__(self):
        pass

    def serial_fields(self, fields, tablename):
        """
        :param fields:
        :param tablename:
        :return: tablename.fields
        """
        if fields is None or tablename is None:
            return False
        resql = ""
        for fs in fields:
            resql = resql + ("%s.%s, " % (tablename, fs))
        resql = resql[:-2]
        return resql

    def serial_and_fields(self, fieldsa, valsa, fieldsb, valsb):
        if fieldsa is None or valsa is None or fieldsb is None or valsb is None:
            errstr = "None in serial_where_fields"
            print(errstr)
            return False
        if len(fieldsa) != len(valsa) or len(valsa) != len(fieldsb) or len(fieldsb) != len(valsb):
            errstr = "len(fieldsa) != len(valsa) in serial_where_fields"
            print(errstr)
            return False
        resql = ""
        for index in range(len(fieldsa)):
            resql = resql + ("%s.%s = %s.%s AND " % (fieldsa[index], valsa[index], fieldsb[index], valsb[index]))
        resql = resql[:-4]
        return resql

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

    def get_sql_drop_table(self, tablename):
        resql = "DROP TABLE %s " % tablename
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

    def get_sql_insert_all(self, desttable, destfields, srctable, srcfields):
        if len(srcfields) != len(destfields):
            errstr = "len(fieldsname) != len(fieldsval) in get_sql_insert_one"
            print(errstr)
            return False
        sqlsrc = ""
        sqldest = ""
        for index in range(len(srcfields)):
            sqlsrc = sqlsrc + ("%s, " % srcfields[index])
            sqldest = sqldest + ("%s, " % destfields[index])
        sqlsrc = sqlsrc[:-2]
        sqldest = sqldest[:-2]
        resql = " INSERT INTO %s (%s) SELECT %s FROM %s " % (srctable, sqlsrc, desttable, sqldest)
        return resql

    def get_sql_delete_where(self, tablename, wherestatement):
        resql = "DELETE t.* FROM %s AS t WHERE (%s) " % (tablename, wherestatement)
        return resql

    def get_sql_delete(self, tablename):
        resql = "DELETE * FROM %s" % tablename
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

    def get_sql_selectinto(self, srctable, srcfields, desttablename):

        resql = " SELECT %s INTO %s FROM %s WHERE (%s) " % ()
        return resql

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

    def get_sql_update(self, tablename, setfield, setval, wherefields, wherevals):
        if len(wherefields) != len(wherevals):
            errstr = "len(wherefields) != len(wherevals) get_sql_update"
            print(errstr)
            return False
        if tablename is None or setfield is None or setval is None or wherefields is None or wherevals is None:
            errstr = "None get_sql_update"
            print(errstr)
            return False
        # UPDATE PatientDischargeSummary AS t SET t.LsContent = \'%s\' WHERE
        setval = setval.replace("\'", "\"")
        setstr = " t.%s = \'%s\' " % (setfield, setval)
        sherestr = ""
        for index in range(wherefields):
            sherestr = sherestr + ("t.%s = \'%s\' AND " % (wherefields[index], wherevals[index]))
        sherestr = sherestr[:-4]
        resql = " UPDATE %s AS t SET %s WHERE (%s) " % (tablename, setstr, sherestr)
        return resql


if __name__ == '__main__':
    print("test sqlaccess")
    showfields = ['patientid', 'name', 'examdatetime']
    table = 'PatientInfo'
    timefield = 'examdatetime'
    objsql = SqlAccess()
    print(objsql.get_sql_mindatetime(showfields, table, timefield))
