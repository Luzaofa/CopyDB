#!/usr/bin/env python
# encoding: utf-8
# Time    : 1/3/2019 11:18 AM
# Author  : Luzaofa

import time
import os
import sys
import datetime
import multiprocessing as mp
from apscheduler.schedulers.blocking import BlockingScheduler

from DBHelper import DB_Helper
import windConfigCheck as windConfig

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

    def count_diff_object_id_number(self, tableName, logName, startTime, endTime):
        object_id = {}
        mass = 'Error'
        sql = windConfig.count_db_number_id.format(tableName=tableName, startTime=startTime, endTime=endTime)
##        print sql
        for key, value in windConfig.DB_Mass.items():
            data = UpdateDB.select(hose=value['host'], user=value['user'], password=value['password'],
                                   database=value['database'], charset=value['charset'], sql=sql)
            _id = []
            for i in data['rows']:
                _id.append(i[0])
            object_id[key] = _id
        if len(object_id['09']) == len(object_id['21']):
            mass = 'Success'
        _log = {'09': len(object_id['09']), '21': len(object_id['21'])}
        self.log('tableName: ' + tableName + '; Database status：' + str(_log) + ' ' + mass + '\n', logName)
        if len(object_id['09']) == len(object_id['21']):
            return 0, object_id

        return object_id, object_id

    def find_diff_id(self, tableName, logName, startTime, endTime):
        ids, object_id = self.count_diff_object_id_number(tableName, logName, startTime, endTime)
        if not ids:
            return 0, object_id
        diff = tuple(set(ids['09']).difference(set(ids['21'])))
        return diff, ids

    def split_work(self, tableName, logName, startTime, endTime):

        diff_ids, ids = self.find_diff_id(tableName, logName, startTime, endTime)
        if not diff_ids:
            mass = 'Error'
            if len(ids['09']) == len(ids['21']):
                mass = 'Success'
            print mass

            check_delete = windConfig.check_delete.format(TABLENAME=tableName)
            delete_comstr = "sqlcmd -d wind -Q \"" + check_delete + "\""
            os.system(delete_comstr)

            check_insert = windConfig.check_insert.format(UP_DATE=time.strftime("%Y-%m-%d", time.localtime()),
                                                          CREATETIME=time.strftime("%H:%M:%S", time.localtime()),
                                                          TABLENAME=tableName, DATANUM09=len(ids['09']),
                                                          DATANUM24=len(ids['21']), STATECODE=mass)
            insert_comstr = "sqlcmd -d wind -Q \"" + check_insert + "\""
            os.system(insert_comstr)

            print 'OK'
            return 0

        print 'Updating now, please wait.......'

        split_num = 150
        start = 0
        for i in range(len(diff_ids) / split_num + 1):
            diff_ids_ = diff_ids[start: start + split_num]
            if len(diff_ids_) != 0:
                self.download_data(tableName, logName, startTime, endTime, diff_ids_)
                mass = 'success!'
                if not self.upload_data(tableName, logName, startTime, endTime, diff_ids_):
                    mass = 'error!'
                print mass
                # time.sleep(1)
            start += split_num
        return 1

    def download_data(self, tableName, logName, startTime, endTime, diff_ids):
        start = time.time()
        mass = windConfig.DB_Mass['09']
        outFile = '{path}/{fileName}.out'.format(path=self.check_file('data'), fileName=tableName)
        if os.path.exists(outFile):
            os.remove(outFile)
        # if tableName != 'TB_OBJECT_9003':
        #     dis = self.count_db_number(tableName=tableName, startTime=startTime, endTime=endTime)
        #     self.log('tableName: ' + tableName + ', current time：' + time.strftime("%Y-%m-%d.%H:%M:%S",
        #                                                                            time.localtime()) + '; Pre-update database status：' + str(
        #         dis) + '\n', logName)
        #     if dis['09'] == dis['21']:
        #         self.log('The same data! Update Successful!' + '\n', logName)
        #         return False
        if len(diff_ids) == 1:
            diff_ids = str(diff_ids).replace(',', '')
        sql = windConfig.download_data.format(tableName=tableName, startTime=startTime, endTime=endTime,
                                              object_ids=str(diff_ids).replace('u', ''))
        if tableName == 'TB_OBJECT_9003':
            sql = windConfig.download_data_9003.format(tableName=tableName, startTime=startTime, endTime=endTime,
                                                       object_ids=str(diff_ids).replace('u', ''))
        comstr = "bcp \"" + sql + " \" queryout " + outFile + " -S " + \
                 mass['host'] + " -U " + mass['user'] + " -P \"" + mass['password'] + "\" -n "

        if os.system(comstr) != 0:
            self.log('Downloading：' + tableName + 'SQL：' + str(comstr) + '\n', logName)
            self.log('Error data download' + '\n', logName)
            return False
        all_time = str((time.time() - start) / 60) + 'Min； '

        self.log('Downloading：' + tableName + '； time cost：' + all_time + 'SQL：' + str(comstr) + '\n', logName)
        return True

    def upload_data(self, tableName, logName, startTime, endTime, diff_ids_):

        targettableName = tableName
        path = 'data/' + time.strftime("%Y-%m-%d", time.localtime()) + '/' + tableName + '.out'

        if os.path.isfile(path):
            start = time.time()
            # tableName = 'TB_OBJECT_1079_test'  # testTable
            # mass = windConfig.DB_Mass['21']
            upFile = 'data/{path}/{fileName}.out'.format(path=time.strftime("%Y-%m-%d", time.localtime()),
                                                         fileName=tableName)

            if tableName == 'TB_OBJECT_9003':
                tableName = 'TB_OBJECT_9003_2019'

            sql = windConfig.delete_data.format(tableName=tableName, object_ids=str(diff_ids_).replace('u', ''))
            delete_comstr = "sqlcmd -d wind -Q \"" + sql + "\""
            self.log('delete: ' + str(delete_comstr) + '\n', logName)
            os.system(delete_comstr)
            comstr = "bcp wind.dbo." + tableName + " in " + upFile + " -T -n "
            comstr_24 = "bcp wind.dbo." + tableName + " in " + upFile + " -S 192.168.205.24 -U windadmin -P \"wind2018\"  -n "
            
            bcplogFile = '{path}/{fileName}.out'.format(path=self.check_file('bcp_log'), fileName=targettableName)
            if os.path.exists(bcplogFile):
                os.remove(bcplogFile)

            comstr = comstr + " 1>" + bcplogFile
            comstr_24 = comstr_24 + " 1>" + bcplogFile
            
            if os.system(comstr) != 0:
                self.log('Error data upload' + '\n', logName)
                return False
            self.log('21_upload: ' + str(comstr) + '\n', logName)
            
            if os.system(comstr_24) != 0:
                self.log('Error data upload' + '\n', logName)
                return False
            self.log('24_upload: ' + str(comstr_24) + '\n', logName)
            

            if tableName != 'TB_OBJECT_9003':
                newDis = self.count_db_number(tableName=targettableName, startTime=startTime, endTime=endTime)

                mass = 'Error'
                if float(newDis['09']) == float(newDis['21']):
                    mass = 'Success'

                check_delete = windConfig.check_delete.format(TABLENAME=tableName)
                delete_comstr = "sqlcmd -d wind -Q \"" + check_delete + "\""
                os.system(delete_comstr)

                check_insert = windConfig.check_insert.format(UP_DATE=time.strftime("%Y-%m-%d", time.localtime()),
                                                              CREATETIME=time.strftime("%H:%M:%S", time.localtime()),
                                                              TABLENAME=tableName, DATANUM09=newDis['09'],
                                                              DATANUM24=newDis['21'], STATECODE=mass)
                insert_comstr = "sqlcmd -d wind -Q \"" + check_insert + "\""
                os.system(insert_comstr)

                self.log('tableName: ' + tableName + ', current time：' + time.strftime("%Y-%m-%d.%H:%M:%S",
                                                                                       time.localtime()) + '; Updated database status：' + str(
                    newDis) + '\n', logName)
                all_time = str((time.time() - start) / 60) + 'Min； '
                print comstr
                self.log('21_Uploading：' + targettableName + '； time cost：' + all_time + 'SQL：' + str(comstr) + '\n', logName)
                print comstr_24
                self.log('24_Uploading：' + targettableName + '； time cost：' + all_time + 'SQL：' + str(comstr_24) + '\n', logName)
                return True

    def data_mp(self, pro, tableNames, logName, func, startTime, endTime):
        pool = mp.Pool(processes=pro)
        for tableName in tableNames:
            print 'Checking: {0}'.format(tableName)
            pool.apply_async(func, args=(tableName, logName, startTime, endTime))
        pool.close()
        pool.join()

    # def data_mp(self, pro, tableNames, logName, func, startTime, endTime):
    #     for tableName in tableNames:
    #         print 'Checking: {0}'.format(tableName)
    #         code = func(tableName, logName, startTime, endTime)
    #         if not code:
    #             continue

    def main_mp(self, tableNames, logName, startTime, endTime):

        main_start = time.time()

        cpu_count = 4
        logName = '{path}/{fileName}.txt'.format(path=helper.check_file('check'),
                                                 fileName=time.strftime("%H-%M-%S", time.localtime()))
        self.data_mp(cpu_count, tableNames, logName, self.split_work, startTime, endTime)

        main_end = time.time()
        print 'All Time: ' + str((main_end-main_start)/60) + ' min'

        print 'Next Data synchronization: ', (datetime.datetime.now() + datetime.timedelta(hours=1)).strftime(
            "%Y-%m-%d %H:%M:%S")
        # self.data_mp(cpu_count, tableNames, logName, self.upload_data, startTime, endTime)


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

    startTime = datetime.datetime.strptime(startTime, '%Y-%m-%d')
    endTime = datetime.datetime.strptime(endTime, '%Y-%m-%d')

    logName = '{path}/{fileName}.txt'.format(path=helper.check_file('check'),
                                             fileName=time.strftime("%H-%M-%S", time.localtime()))
    helper.main_mp(tableNames, logName, startTime, endTime)
    # print 'end'
    # time.sleep(10)

    sched = BlockingScheduler()

    sched.add_job(helper.main_mp, 'interval', seconds=60 * 30 * 1, args=[tableNames, logName, startTime, endTime], id='main_mp')

    sched.start()







    
