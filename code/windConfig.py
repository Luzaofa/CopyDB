#!/usr/bin/env python
# encoding: utf-8
# Time    : 1/3/2019 12:34 PM
# Author  : Luzaofa


allList = ['XXX', 'XXX']

count_db_number = "select count(*) XXX from {tableName} where XXX >= '{startTime}' and XXX <= '{endTime}'"

download_data = "select * from XXX.dbo.{tableName} where XXX >= '{startTime}' and XXX <= '{endTime}'"

upload_data = "delete from XXX.dbo.{tableName} where XXX >= '{startTime}' and XXX <= '{endTime}'"

DB_Mass = {
    'A': {'host': '192.168.205.XXX', 'user': 'XXX', 'password': 'XXX', 'database': 'XXX',
           'charset': 'utf8'},
    'B': {'host': '192.168.205.XXX', 'user': 'XXX', 'password': 'XXX', 'database': 'XXX', 'charset': 'utf8'}
}
