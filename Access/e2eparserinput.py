"""
It is processed for parser the string from xml in PatientDischargeSummary Table.
"""

import xmlfordischarge
import access
import os

def PatientDischargeSummaryXMLParser():
    dbpath = r'C:\lwz\softproject\py\data\testprocessxml\Localsrc.mdb'
    dbpath = os.path.abspath(dbpath)
    resbase = " 入院情况及诊疗过程:\n %s\n出院情况:%s\n"
    sqlbaseupdate = " UPDATE PatientDischargeSummary AS t SET t.LsContent = \'%s\' WHERE t.PatientId = \'%s\' AND t.VisitId = \'%s\';"
    objdb = access.Access(dbpath)

    dbpathupsate = r'C:\lwz\softproject\py\data\testprocessxml\Localdest.mdb'
    objdbupdate = access.Access(os.path.abspath(dbpathupsate))

    sqlselect = "SELECT t.PatientId, t.VisitId, t.LsContent FROM PatientDischargeSummary AS t;"
    if not objdb.sql_excute(sqlselect):
        raise RuntimeError("sql_excute %s" % sqlselect)
    while True:
        row = objdb.sql_cur_fetchone()
        if not row:
            print("error in row")
            break
        pid = row['patientid']
        vid = row['visitid']
        objxml = xmlfordischarge.XmlParseString(row['lscontent'])
        xmlin = objxml.getFirstTagString('InState')
        xmlout = objxml.getFirstTagString('OutState')
        xmlres = resbase % (xmlin, xmlout)
        xmlres = xmlres.replace("\'", "\"")
        ressql = sqlbaseupdate % (xmlres, pid, vid)
        if not objdbupdate.sql_update(ressql):
            raise RuntimeError("sql_update %s" % ressql)


if __name__ == '__main__':
    print('start parser the string from xml in PatientDischargeSummary Table.')
    PatientDischargeSummaryXMLParser()
