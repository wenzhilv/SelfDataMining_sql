
import xmlfordischarge
import access
import os
import traceback

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


if __name__ == '__main__':
    print('start liu')
    first_abnorm_us('sadas')
    print('end liu')
