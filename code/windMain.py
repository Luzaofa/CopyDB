#!/usr/bin/env python
# encoding: utf-8
# Time    : 1/3/2019 11:18 AM
# Author  : Luzaofa

import time
import os
import sys
import multiprocessing as mp

from DBHelper import DB_Helper
import windConfig

import types
import copy_reg


def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copy_reg.pickle(types.MethodType, _pickle_method)


class UpdateDB(DB_Helper):

    def __init__(self):
        super(DB_Helper, self).__init__()

    @classmethod
    def select(cls, hose, user, password, database, charset, sql):
        data = DB_Helper(hose, user, password, database, charset).query(sql, None)
        return data

    def log(self, logMass, logName):
        with open(logName, 'a+') as log:
            log.writelines(logMass)
            log.flush()

    def check_file(self, root_):
        path = root_ + '/' + time.strftime("%Y-%m-%d", time.localtime())
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def count_db_number(self, tableName, startTime, endTime):
        dis = {}
        sql = windConfig.count_db_number.format(tableName=tableName, startTime=startTime, endTime=endTime)
        for key, value in windConfig.DB_Mass.items():
            data = UpdateDB.select(hose=value['host'], user=value['user'], password=value['password'],
                                   database=value['database'], charset=value['charset'], sql=sql)
            dis[key] = data['rows'][0][0]
        return dis

    def download_data(self, tableName, logName, startTime, endTime):
        start = time.time()
        mass = windConfig.DB_Mass['09']
        outFile = '{path}/{fileName}.out'.format(path=self.check_file('data'), fileName=tableName)
        if os.path.exists(outFile):
            os.remove(outFile)
        sql = windConfig.download_data.format(tableName=tableName, startTime=startTime, endTime=endTime)
        comstr = "bcp \"" + sql + " \" queryout " + outFile + " -S " + \
                 mass['host'] + " -U " + mass['user'] + " -P \"" + mass['password'] + "\" -n "
        if os.system(comstr) != 0:
            return False
        all_time = str((time.time() - start) / 60) + 'S； '
        print comstr
        self.log('Downloading：' + tableName + '； time cost：' + all_time + 'SQL：' + str(comstr) + '\n', logName)
        return True

    def upload_data(self, tableName, logName, startTime, endTime):
        start = time.time()
        # tableName = 'TB_OBJECT_1079_test'  # testTable
        # mass = windConfig.DB_Mass['24']
        upFile = 'data/{path}/{fileName}.out'.format(path=time.strftime("%Y-%m-%d", time.localtime()),
                                                     fileName=tableName)
        sql = windConfig.upload_data.format(tableName=tableName, startTime=startTime, endTime=endTime)
        delete_comstr = "sqlcmd -d wind -Q \"" + sql + "\""
        print delete_comstr
        if os.system(delete_comstr) != 0:
            return False
        comstr = "bcp wind.dbo." + tableName + " in " + upFile + " -T -n "
        if os.system(comstr) != 0:
            return False
        all_time = str((time.time() - start) / 60) + 'S； '
        print comstr
        self.log('Uploading：' + tableName + '； time cost：' + all_time + 'SQL：' + str(comstr) + '\n', logName)
        return True
    
    def data_mp(self, tableNames, logName, func, startTime, endTime):
        pool = mp.Pool(processes=4)
        for tableName in tableNames:
            pool.apply_async(func, args=(tableName, logName, startTime, endTime))
        pool.close()
        pool.join()

    def main(self, tableNames, logName, startTime, endTime):

        for tableName in tableNames:
            dis = self.count_db_number(tableName=tableName, startTime=startTime, endTime=endTime)
            self.log('current time：' + time.strftime("%Y-%m-%d.%H:%M:%S",
                                                     time.localtime()) + '; Pre-update database status：' + str(
                dis) + '\n', logName)
            if self.download_data(tableName, logName, startTime, endTime):
                self.log('Successful data download' + '\n', logName)
            else:
                self.log('Error data download' + '\n', logName)

            if self.upload_data(tableName, logName, startTime, endTime):
                self.log('Successful data upload' + '\n', logName)
            else:
                self.log('Error data upload' + '\n', logName)
            newDis = self.count_db_number(tableName=tableName, startTime=startTime, endTime=endTime)
            self.log(
                'current time：' + time.strftime("%Y-%m-%d.%H:%M:%S",
                                                time.localtime()) + '; Updated database status：' + str(
                    newDis) + '\n', logName)
            self.log('\n', logName)

    # def main(self, tableName, logName, startTime, endTime):
    #
    #     dis = self.count_db_number(tableName=tableName, startTime=startTime, endTime=endTime)
    #     self.log('current time：' + time.strftime("%Y-%m-%d.%H:%M:%S",
    #                                              time.localtime()) + '; Pre-update database status：' + str(
    #         dis) + '\n', logName)
    #     if self.download_data(tableName, logName, startTime, endTime):
    #         self.log('Successful data download' + '\n', logName)
    #     else:
    #         self.log('Error data download' + '\n', logName)
    #
    #     if self.upload_data(tableName, logName, startTime, endTime):
    #         self.log('Successful data upload' + '\n', logName)
    #     else:
    #         self.log('Error data upload' + '\n', logName)
    #     newDis = self.count_db_number(tableName=tableName, startTime=startTime, endTime=endTime)
    #     self.log(
    #         'current time：' + time.strftime("%Y-%m-%d.%H:%M:%S",
    #                                         time.localtime()) + '; Updated database status：' + str(
    #             newDis) + '\n', logName)
    #     self.log('\n', logName)


if __name__ == '__main__':
    helper = UpdateDB()
    print 'start'

    cycle_no = sys.argv
    if len(cycle_no) <= 3:
        print 'Input error, the correct format is: start python code/windMain.py tableName starTime endTime'
        time.sleep(10)
        exit()
    elif len(cycle_no) == 4 and cycle_no[1] == 'all':
        tableNames = windConfig.allList
        startTime = cycle_no[-2]
        endTime = cycle_no[-1]
    else:
        tableNames = list(cycle_no[1:-2])
        startTime = cycle_no[-2]
        endTime = cycle_no[-1]

    logName = '{path}/{fileName}.txt'.format(path=helper.check_file('log'),
                                             fileName=time.strftime("%H-%M-%S", time.localtime()))
    # helper.data_mp(tableNames, logName, helper.main, startTime, endTime)
    helper.main(tableNames, logName, startTime, endTime)
    print 'end'
    time.sleep(10)
