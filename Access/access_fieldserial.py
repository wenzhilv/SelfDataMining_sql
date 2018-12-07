

import numpy as np
import pandas as pd
import logging

import os
import sys
import pypyodbc
import shutil

def testpypyodbc():
    print('start test pypyodbc.')
    DBfile = r'C:\lwz\softproject\jl\项目总结\jzx-cui\process\Local-py.mdb'
    if not os.path.exists(DBfile):
        print('Error: %s Is not exists! ' % DBfile)
        return False
    DBConnStr = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=' + DBfile
    DBConn = pypyodbc.connect(DBConnStr)
    DBCursor = DBConn.cursor()
    sql = r"SELECT top 5 * from ExamInfo;"
    sql2 = """
             SELECT TOP 5 *
             FROM ExamInfo;
         """
    DBCursor.execute(sql)
    tcnt = 1
    while True:
        row = DBCursor.fetchone()
        if not row:
            break
        print('patientId is %s' % row['PatientId'])
        if 0 == (tcnt % 5):
            print('index fetch %d' % tcnt)
        tcnt += 1
        if tcnt > 15:
            break
    DBCursor.close()
    DBConn.close()


def delUsImg():
    RealFileDir = r'D:\img\jzx-cui\_'
    DBfile = r'C:\lwz\softproject\jl\项目总结\jzx-cui\process\Local-py.mdb'
    DBConnStr = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=' + DBfile
    DBConn = pypyodbc.connect(DBConnStr)
    DBCursor = DBConn.cursor()
    sqlBase = r"SELECT count(*) FROM xPathUSLBExImg WHERE xPathUSLBExImg.USApplyNo = "
    dirs = os.listdir(RealFileDir)
    tcnt = 0
    for mydir in dirs:
        sql = sqlBase + '\'' + mydir + '\'' + ';'
        # print( sql )
        DBCursor.execute(sql)
        rows = DBCursor.fetchall()
        rowcnt = rows[0][0]
        if rowcnt == 0:
            print(mydir + ('error and %d ,cnt is %d' % (rowcnt, tcnt)))
            # thefilepath = os.path.join(RealFileDir, mydir)
            # shutil.rmtree(thefilepath)
        else:
            pass
            # print( dir + ('OK and %d ,cnt is %d' %(rowcnt, tcnt) ) )
        tcnt += 1
        if 0 == (tcnt % 100):
            #print(tcnt)
            pass
    DBCursor.close()
    DBConn.close()


if __name__ == '__main__':
    print("start process access")
    # testpypyodbc()
    delUsImg()
    print("stop process access")

