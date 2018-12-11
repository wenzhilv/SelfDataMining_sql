
import xmlfordischarge
from access import Access
import os
import traceback
from sqlaccess import SqlAccess
from xmlfordischarge import XmlParseString

ObjSql = SqlAccess()

JLSTRUCTFIELD = {}
JLSTRUCTFIELD['就诊卡号'] = r'patientid'
JLSTRUCTFIELD['住院号'] = r'visitid'
JLSTRUCTFIELD['影像号'] = r'applyno'
JLSTRUCTFIELD['检查时间'] = r'examdatetime'
JLSTRUCTFIELD['检查映像'] = r'reportimpression'
JLSTRUCTFIELD['检查所见'] = r'reportdescription'
JLSTRUCTTABLE= {}
JLSTRUCTTABLE['患者主索引'] = r'patientlist'
JLSTRUCTTABLE['患者基本信息'] = r'patientinfo'
JLSTRUCTTABLE['检查信息'] = r'examinfo'
JLSTRUCTTABLE['检验信息'] = r'labitems'
JLSTRUCTTABLE['检验结果'] = r'labresults'
JLSTRUCTTABLE['住院诊断'] = r'patientdiag'
JLSTRUCTTABLE['手术'] = r'patientoperation'
JLSTRUCTTABLE['出院小结'] = r'patientdischargesummary'


def get_sql_select(fieldnames, tablename):
    if fieldnames is None or tablename is None:
        return False
    if not isinstance(tablename, str):
        return False
    sqlfrom = " FROM %s;" % tablename
    sqlselect = ""
    for tn in fieldnames:
        if not isinstance(tn, str):
            # errstr = "tn is not string"
            # self.__acs_logger.error(errstr)
            return False
        sqlselect = sqlselect + ("t.%s, " % tn)
    sqlselect = sqlselect[:-2]
    sqlstatement = "SELECT " + sqlselect + sqlfrom
    return sqlstatement

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
        print('get_row_val except')
        traceback.print_exc()
        return False

def first_abnorm_us(dbcur):
    tfield = []
    tfield.append(JLSTRUCTFIELD['就诊卡号'])
    tfield.append(JLSTRUCTFIELD['住院号'])
    tfield.append(JLSTRUCTFIELD['检查时间'])
    sqlstatement = get_sql_select(tfield, JLSTRUCTTABLE['检查信息'])
    print(sqlstatement)

def get_NT_value(objdb):
    fields = ['patientid', 'applyno', 'examdatetime', 'reportimpression', 'reportdescription']
    ntfield = ['reportimpression']
    tablename = 'ExamInfo'
    desttable = 'xExamInfoNT'
    fieldNT = 'NTVal'
    sqlsel = ObjSql.serial_fields(fields, 't')
    sqlwhere = "t.ExamItemStr Like '*NT*'"
    sqlNT = " SELECT %s INTO %s FROM %s AS t WHERE (%s)" % (sqlsel, desttable, tablename, sqlwhere)
    if objdb.sql_excute(("SELECT TOP 1 * FROM %s" % desttable)):
        sqldrop = ObjSql.get_sql_drop_table(desttable)
        objdb.sql_update(sqldrop)
    else:
        RunError(objdb, "get_sql_drop_table(desttable)")
        # RuntimeError("get_sql_drop_table(desttable)")
    if not objdb.sql_update(sqlNT):
        RunError(objdb, "get_NT_value sql_update(sqlNT)")
        # RuntimeError("get_NT_value sql_update(sqlNT)")
    sqladd = ObjSql.get_sql_add_field(desttable, fieldNT, 50)
    if not objdb.sql_update(sqladd):
        RunError(objdb, "get_sql_add_field(desttable, fieldNT, 50)")
        # RuntimeError("get_sql_add_field(desttable, fieldNT, 50)")
    sqlgetNT = " UPDATE %s AS t SET t.%s = MID(t.%s, INSTR(t.%s, 'NT'), 8)" % (desttable, fieldNT, ntfield, ntfield)
    if not objdb.sql_update(sqlgetNT):
        RunError(objdb, "get_NT_value sql_update(sqlNT)")
        # RuntimeError("get_NT_value sql_update(sqlNT)")
    pass


def get_NT_value2(objdb):
    fields = ['patientid', 'applyno', 'examdatetime', 'reportimpression', 'reportdescription', 'ExamItemStr']
    ntsrcfield = 'reportdescription'
    tablename = 'ExamInfo'
    desttable = 'xExamInfoNT'
    fieldNT = 'NTVal'
    if objdb.sql_excute(("SELECT TOP 1 * FROM %s" % desttable)):
        sqldrop = ObjSql.get_sql_drop_table(desttable)
        objdb.sql_update(sqldrop)
    else:
        print(desttable+"is not exists")
    sqlsel = ObjSql.serial_fields(fields, 't')
    sqlNT = " SELECT %s INTO %s FROM %s AS t " % (sqlsel, desttable, tablename)
    if not objdb.sql_update(sqlNT):
        # RuntimeError("get_NT_value sql_update(sqlNT)")
        RunError(objdb, "error sql_update(sqlNT)")
    sqlntdel = ObjSql.get_sql_delete_where(desttable, " t.ExamItemStr  NOT LIKE \'%NT%\' ")
    if not objdb.sql_update(sqlntdel):
        # RuntimeError("get_NT_value sql_update(sqlntinsert)")
        RunError(objdb, "get_NT_value sql_update(sqlntinsert)")
    sqladd = ObjSql.get_sql_add_field(desttable, fieldNT, 50)
    if not objdb.sql_update(sqladd):
        # RuntimeError("get_sql_add_field(desttable, fieldNT, 50)")
        RunError(objdb, "get_sql_add_field(desttable, fieldNT, 50)")
    sqlgetNT = " UPDATE %s AS t SET t.%s = MID(t.%s, INSTR(t.%s, \'NT\'), 8)" % (desttable, fieldNT, ntsrcfield, ntsrcfield)
    sqlgetNT += (" WHERE t.%s LIKE \'%%NT%%\'" % ntsrcfield)
    if not objdb.sql_update(sqlgetNT):
        # RuntimeError("get_NT_value sql_update(sqlgetNT)")
        RunError(objdb, "get_NT_value sql_update(sqlgetNT)")
    pass


def get_NT_value3(objdb):
    # fields = ['patientid', 'applyno', 'examdatetime', 'reportimpression', 'reportdescription', 'ExamItemStr']
    fields = ['patientid', 'applyno', 'examdatetime', 'reportimpression', 'reportdescription']
    ntsrcfield = 'reportdescription'
    tablename = 'ExamInfo'
    desttable = 'xExamInfoNT'
    fieldNT = 'NTVal'
    if objdb.sql_excute(("SELECT TOP 1 * FROM %s" % desttable)):
        sqldrop = ObjSql.get_sql_drop_table(desttable)
        objdb.sql_update(sqldrop)
    else:
        print(desttable+"is not exists")

    sqlsel = ObjSql.serial_fields(fields, 't')
    sqlNT1 = " SELECT %s, MID(t.%s, INSTR(t.%s, \'NT\'), 9) AS %s INTO %s FROM %s AS t " % (sqlsel, ntsrcfield, ntsrcfield, fieldNT, desttable, tablename)
    sqlNT2 = " WHERE ( t.%s LIKE \'%%NT%%\' AND t.%s LIKE \'%%NT%%\') " % ('ExamItemStr', ntsrcfield)
    sqlNT = sqlNT1 + sqlNT2
    if not objdb.sql_update(sqlNT):
        # RuntimeError("get_NT_value sql_update(sqlNT)")
        RunError(objdb, "error sql_update(sqlNT)")
    pass


def RunError(objdb, str):
    objdb.dbclose()
    RuntimeError("get_NT_value sql_update(sqlgetNT)")


def get_ntAndapgar_Discharge(objdb):
    """
    get information from dischargesummary, NT and apgar score
    :param objdb:
    :return:
    """
    # objxml = xmlfordischarge.XmlParseString(row['lscontent'])
    fields = ['patientid', 'visitid', 'LsContent']
    tablename = r'PatientDischargeSummary'
    fieldinstr = 'LsContent'
    fielddestinstr = 'InState'
    fieldApgar = 'ApgarScore'
    apgarstart = '<InState'
    apgarend = '</InState>'
    desttable = 'xDischarge'
    if objdb.sql_excute(("SELECT TOP 1 * FROM %s" % desttable)):
        sqldrop = ObjSql.get_sql_drop_table(desttable)
        objdb.sql_update(sqldrop)
    else:
        print("xDischarge is not exists")
    sqlsel = ObjSql.serial_fields(fields, 't')

    sqlinstrstart = " INSTR(t.%s, \'%s\') " % (fieldinstr, apgarstart)
    sqlinstrend = " INSTR(t.%s, \'%s\') " % (fieldinstr, apgarend)
    sqlapgarmid = " MID(t.%s, %s+28, (%s-%s)) " % (fieldinstr, sqlinstrstart, sqlinstrend, sqlinstrstart)
    sqlApgar = " SELECT %s, %s AS %s INTO %s FROM %s AS t  WHERE ( %s <> 0)" % (sqlsel, sqlapgarmid, fielddestinstr, desttable, tablename, sqlinstrstart)
    if not objdb.sql_update(sqlApgar):
        # RuntimeError("(sqlApgar)")
        RunError(objdb, "(sqlntinsert)")


if __name__ == '__main__':
    print('start liu')
    dbsrc = r"D:\lwz\softproject\py\process data\fck-trszyc\python process\Localsrctmp.mdb"
    objDB = Access(dbsrc)
    # get_NT_value3(objDB)
    get_ntAndapgar_Discharge(objDB)
    objDB.dbclose()
    print('end liu')
