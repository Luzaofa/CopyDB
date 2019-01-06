#!/usr/bin/env python
# encoding: utf-8
# Time    : 12/29/2018 11:15 AM
# Author  : Luzaofa

import pymssql as mdb


class DB_Helper(object):
    conn = None

    def __init__(self, hose, user, password, database, charset):
        self.host = hose
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset

    @classmethod
    def reset_init_(cls, hose, user, password, database, charset):
        cls(hose, user, password, database, charset)

    def connect(self):
        try:
            self.conn = mdb.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset
            )
            return True
        except AttributeError as e:
            return False

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def deal(self, code, data=None):
        if code == -1:
            print {'code': -1, 'errormsg': 'error'}
            return 0
        else:
            print {'code': 1, 'data': data}
            return 0

    def create_table(self, sql):  # list
        # sql = "create table luzaofa(PID int NOT NULL PRIMARY KEY, Mas CHAR(100))"
        self.public_operation(sql)

    def query(self, sql, args=None):
        if not self.connect():
            self.deal(-1)
        res = {}
        with self.conn.cursor() as cursor:
            cursor.execute(sql, args)
            try:
                res['rows'] = cursor.fetchall()
                res['columns'] = [col[0] for col in cursor.description]
            except IndexError as e:
                print(str(e))
        self.close()
        return res

    def batch_insert(self, sql, param):  # list
        # sql = "insert into zaofa(name, age, phone) values (?, ?, ?)"   param = [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
        self.public_operation(1, sql, param)

    def delete(self, sql):
        # sql = "delete from zaofa where name = 'luzaofa'"
        self.public_operation(sql)

    def update(self, sql):
        # sql = "update luzaofa set Mas = '%s' where PID = 1" % ('luzaofa')
        self.public_operation(sql)

    def public_operation(self, code=0, sql='', param=None):
        if not self.connect():
            self.deal(-1)
        with self.conn.cursor() as cursor:
            try:
                if code == 1:
                    cursor.executemany(sql, param)
                else:
                    cursor.execute(sql, param)
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()
        self.close()
        return 0
