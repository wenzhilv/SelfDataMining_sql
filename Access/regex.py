
import re
from access import Access
from sqlaccess import SqlAccess
import glob
import os
import shutil
import simplelogger
import time


TABLENAME = {}
TABLENAME['深圳二医院'] = 'xExam_SZ'
TABLENAME['襄阳'] = 'xExam_XY'
TABLENAME['浙江大学'] = 'xExam_ZD'
TABLENAME['中山附一'] = ''
HOSPITALNAME = {}
HOSPITALNAME['深圳二医院'] = r'K:\JL\图像\zsq\深圳二医院\Images\*\SC'
HOSPITALNAME['襄阳'] = 'xExam_XY'
HOSPITALNAME['浙江大学'] = 'xExam_ZZD'
HOSPITALNAME['中山附一'] = ''

regexlog = simplelogger.SimpleLogger('resgexlog', "res").get_simple_logger()


def test():
    regex = r"20100411016"
    pattern = re.compile(regex)
    substr = r"K:\JL\图像\zsq\深圳二医院\Images\20100411016_³Â¶¬ºì^^^^\SC\20100411016"
    reflags = re.I
    # match = re.search(pattern, substr, reflags)
    regex2 = r"^\*$\SC"


def show_applyno_match(dbpath, hospitname, imgdir):
    ObjSql = SqlAccess()
    objDB = Access(dbpath)
    # dirs = glob.glob
    sqls = r" SELECT DISTINCT t.ApplyNo FROM %s AS t " % TABLENAME[hospitname]
    paths = ""
    if not objDB.sql_excute(sqls):
        RunError(objDB, sqls)
    emptycnt = 0
    nofilecnt = 0
    cnt = 0
    while True:
        row = objDB.sql_cur_fetchone()
        if not row:
            print("error in row")
            break
        applynostr = row['applyno']
        dirs = os.path.join(imgdir, applynostr)
        paths = glob.glob(dirs)
        if len(paths) == 0 or len(paths) == 2:
            tstr = " %s not exist or multipy %d" % (dirs, len(paths))
            print(tstr)
            s1 = tstr.encode("utf-8")
            regexlog.info(s1)
            emptycnt += 1
            continue

        if 0 == len(os.listdir(paths[0])):
            tstr =" %s have no file " % paths[0]
            print(tstr)
            s1 = tstr.encode("utf-8")
            regexlog.info(s1)
            nofilecnt += 1
            continue
        else:
            tstr = " %s have files " % paths[0]
            s1 = tstr.encode("utf-8")
            regexlog.info(s1)
            cnt += 1
    objDB.dbclose()
    tstr = "empty file %d, no file %d, have file %d" % (emptycnt, nofilecnt, cnt)
    regexlog.info(tstr)
    print(tstr)


def show_applyno_match_ZD(dbpath, hospitname, imgdir):
    ObjSql = SqlAccess()
    objDB = Access(dbpath)
    # dirs = glob.glob
    sqls = r" SELECT DISTINCT t.ApplyNo FROM %s AS t " % TABLENAME[hospitname]
    paths = ""
    if not objDB.sql_excute(sqls):
        RunError(objDB, sqls)
    emptycnt = 0
    nofilecnt = 0
    cnt = 0
    while True:
        row = objDB.sql_cur_fetchone()
        if not row:
            print("error in row")
            break
        applynostr = row['applyno']
        dirs = os.path.join(imgdir, applynostr)
        paths = glob.glob(dirs)
        if len(paths) == 0 or len(paths) == 2:
            tstr = " %s not exist or multipy %d" % (dirs, len(paths))
            print(tstr)
            s1 = tstr.encode("utf-8")
            # regexlog.info(s1)
            regexlog.info("%s not or multipy" % applynostr)
            emptycnt += 1
            continue

        if 0 == len(os.listdir(paths[0])):
            tstr ="%s have no file" % paths[0]
            print(tstr)
            s1 = tstr.encode("utf-8")
            # regexlog.info(s1)
            regexlog.info("%s null" % applynostr)
            nofilecnt += 1
            continue
        else:
            tstr = " %s have files " % paths[0]
            s1 = tstr.encode("utf-8")
            # regexlog.info(s1)
            regexlog.info("%s ok" % applynostr)
            cnt += 1
    objDB.dbclose()
    tstr = "empty file %d, no file %d, have file %d" % (emptycnt, nofilecnt, cnt)
    regexlog.info(tstr)
    print(tstr)


def copy_applyno_match_ZD(dbpath, hospitname, imgdir, destdir):
    ObjSql = SqlAccess()
    objDB = Access(dbpath)
    # dirs = glob.glob
    sqls = r" SELECT DISTINCT t.ApplyNo FROM %s AS t " % TABLENAME[hospitname]
    paths = ""
    if not objDB.sql_excute(sqls):
        RunError(objDB, sqls)
    emptycnt = 0
    nofilecnt = 0
    cnt = 0
    allcnt = 0
    while True:
        allcnt += 1
        row = objDB.sql_cur_fetchone()
        if not row:
            print("error in row")
            break
        applynostr = row['applyno']
        dirs = os.path.join(imgdir, applynostr)
        paths = glob.glob(dirs)
        if len(paths) == 0 or len(paths) == 2:
            tstr = " %s not exist or multipy %d" % (dirs, len(paths))
            print(tstr)
            s1 = tstr.encode("utf-8")
            # regexlog.info(s1)
            regexlog.info("%s not or multipy" % applynostr)
            emptycnt += 1
            continue

        if 0 == len(os.listdir(paths[0])):
            tstr ="%s have no file" % paths[0]
            print(tstr)
            s1 = tstr.encode("utf-8")
            # regexlog.info(s1)
            regexlog.info("%s null" % applynostr)
            nofilecnt += 1
            continue
        else:
            tstr = " %s have files " % paths[0]
            s1 = tstr.encode("utf-8")
            # regexlog.info(s1)
            regexlog.info("%s ok" % applynostr)
            cnt += 1
            srcpath = paths[0]
            destpath = os.path.join(destdir, applynostr)
            if os.path.isdir(destpath):
                logtime = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
                destpath += '_' + logtime
                regexlog.info(destpath)
            shutil.copytree(srcpath, destpath)
        if 0 == (allcnt % 100):
            print(allcnt)
    objDB.dbclose()
    tstr = "empty file %d, no file %d, have file %d, allfile %d" % (emptycnt, nofilecnt, cnt, allcnt)
    regexlog.info(tstr)
    print(tstr)


def RunError(objdb, err):
    objdb.dbclose()
    RuntimeError(err)

def cp_zd_usimg():
    hospitname = '浙江大学'
    dbpath = r"D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\提取\Local-zeng.mdb"
    srccpdir = r'D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\图像\浙大\浙大\usimage\*'
    destcpdir = r"H:\lwz\JL\project\image\fck-zsq\dest\zheda\usimage"
    copy_applyno_match_ZD(dbpath, hospitname, srccpdir, destcpdir)


def cp_zd_nomalimage():
    hospitname = '浙江大学'
    dbpath = r"D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\提取\Local-zeng.mdb"
    srccpdir = r'D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\图像\浙大\浙大\nomalimage\_'
    destcpdir = r"H:\lwz\JL\project\image\fck-zsq\dest\zheda\nomalimage"
    copy_applyno_match_ZD(dbpath, hospitname, srccpdir, destcpdir)


def cp_zd_image():
    hospitname = '浙江大学'
    dbpath = r"D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\提取\Local-zeng.mdb"
    srccpdir = r'D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\图像\浙大\浙大\Image\*'
    destcpdir = r"H:\lwz\JL\project\image\fck-zsq\dest\zheda\Image"
    copy_applyno_match_ZD(dbpath, hospitname, srccpdir, destcpdir)


def cp_xy_normal():
    hospitname = '襄阳'
    dbpath = r"D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\提取\Local-zeng.mdb"
    srccpdir = r'D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\图像\襄阳\Normal\Image'
    destcpdir = r"H:\lwz\JL\project\image\fck-zsq\dest\xiangyang\Normal"
    copy_applyno_match_ZD(dbpath, hospitname, srccpdir, destcpdir)


def cp_xy_abnormal():
    hospitname = '襄阳'
    dbpath = r"D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\提取\Local-zeng.mdb"
    srccpdir = r'D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\图像\襄阳\abnormal\Image'
    destcpdir = r"H:\lwz\JL\project\image\fck-zsq\dest\xiangyang\abnormal"
    copy_applyno_match_ZD(dbpath, hospitname, srccpdir, destcpdir)


def cp_sz():
    hospitname = '深圳二医院'
    dbpath = r"D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\提取\Local-zeng.mdb"
    srccpdir = r'D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\图像\深圳二医院\Images\*\SC'
    destcpdir = r"H:\lwz\JL\project\image\fck-zsq\dest\shenzhen"
    copy_applyno_match_ZD(dbpath, hospitname, srccpdir, destcpdir)


if __name__ == '__main__':
    print("start regex")
    dbpath = r"D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\提取\Local-zeng.mdb"
    # show_applyno_match(dbpath, '深圳二医院', r'K:\JL\图像\zsq\深圳二医院\Images\*\SC')
    # show_applyno_match_ZD(dbpath, '浙江大学', r'D:\lwz\onedrive\工作\相关公司网络产品\JL-tmp20181126\project\fck-zeng\图像\浙大\浙大\usimage\*')
    # cp_zd_nomalimage()  #zero
    cp_sz()
    print("end regex")
