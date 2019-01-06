

使用说明：

启动程序需配置好：start_wind.bat，双击即可执行数据库更新

配置说明：

start python code/windMain.py tableName starTime endTime

tableName：数据库表名，可多个一起 eg: XXX1 XXX2
starTime：开始日期 eg:  2018-12-1
endTime：结束日期  eg:  2019-01-03


完整示例语句： 
1、单个数据表更新： start python code/windMain.py XXX 2018-12-1 2019-01-03
2、多个数据表更新： start python code/windMain.py XXX1 XXX2 2018-12-1 2019-01-03
3、全部数据库更新： start python code/windMain.py all 2018-12-1 2019-01-03
