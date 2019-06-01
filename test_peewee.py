#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 4/26/2019 4:13 PM
# @Author  : Steven
# @Site    : 
# @File    : test_peewee.py
# @Software: PyCharm
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 4/26/2019 2:21 PM
# @Author  : Steven
# @Site    :
# @File    : peewee.py
# @Software: PyCharm
from peewee import Model
from peewee import MySQLDatabase
from peewee import CharField, FloatField,IntegerField,DateTimeField
import time

import datetime
db = MySQLDatabase(
    database = 'python',
    host = '127.0.0.1',
    user = 'python',
    passwd = 'python'
)
#继承peewee.Model，创建一个表。
#peewee创建数据库的时候，默认会添加主键id
#peewee创建数据库字段默认不可为空
# 一个基础Model，没别的作用
# 如果一个数据库中有多个不同的表，其中很多内容有一样，就可以通过基础类全部指定好
# 然后不同的Model简单的一继承就好了
class BaseModel(Model):
    ENT_ID = CharField(max_length=500,default='for')
    REGISTERED_NO = CharField(max_length=500,default='xuegod')
    CREDIT_CODE = CharField(max_length=500)
    #将表和数据库连接
    class Meta:
        database = db
class HPA(BaseModel):
    ENT_NAME = CharField(max_length=500)
    db_table = 'HPA'
    order_by = ('CREDIT_CODE',)
#记录函数运行时间
def print_run_time(func):
    def wrapper(*args, **kw):
        local_time = time.time()
        func(*args, **kw)
        print('current Function [%s] run time is %.2f' % (func.__name__, time.time() - local_time))
    return wrapper
class Builk(object):
    def __init__(self,csv):
        #self.table_name = 'HPA'
        self.csv = csv

    @staticmethod
    def creat_new_table(table):
        u"""用来创建table的."""
        # # 如果，该Model代表的table没有被创建，就新建一个
        if not table.table_exists():
            table.create_table()
    def deal_file(self):
        u"""打开csv文件，然后返回信息"""
        with open(self.csv,encoding= 'gbk') as t_csv:
            for i in t_csv:
                line_data = t_csv.readline()
                yield [ i.replace('\n','') for i in line_data.split('\t')]
    @print_run_time  #装饰器记录函数运行时间
    def db_insert(self):
        # 创建HPA这个表
        self.creat_new_table(HPA)
        """最普通的insert方式."""
        with db.atomic():
            for i in self.deal_file():
                try:
                    HPA.insert(ENT_ID = i[0],REGISTERED_NO = i[1],CREDIT_CODE = i[2],ENT_NAME = i[3]).execute()
                except Exception as E:
                    print(E)
                    continue


    def db_insert_dict_v2(self):
        self.creat_new_table(HPA)
        desc_list = ['ENT_ID','REGISTERED_NO','CREDIT_CODE','ENT_NAME']
        for i in self.deal_file():
            yield {k:v.replace('"','') for k,v in zip(desc_list,i)}

    @print_run_time
    def insert_(self):
        u"""测试atomic()."""
        # 新建表
        self.creat_new_table(HPA)
        # 上下文管理一下，使用了atomic()之后，就会在上下文退出的时候才最终commit
        # 减少了中间最耗时的反复commit过程
        with db.atomic():
            # 然后将处理好的字典一次导入数据库
            for data_dict in self.db_insert_dict_v2():
                HPA.create(**data_dict)

    @print_run_time
    def insert_many(self):
        u"""测试insert many."""
        self.creat_new_table(HPA)
        # 基本上就是，将每一行的数据转化成一个字典，然后这些个字典构成一个list
        # 直接导入，即可
        # 最懒的写法，内存耗费最大，完全浪费了生成器节省内存的有点，我图什么？？
        data = [x for x in self.db_insert_dict_v2()]
        #print(data)
        #一次性导入,如果数据大的话要for循环分批导入
        with db.atomic():
            HPA.insert_many(data).execute()


if __name__ == '__main__':

    a = Builk('111.csv')
    a.insert_many()
    # HPA.create_table()
    # if not HPA.table_exists():
    #  HPA.create_table()
    # a = Builk('5')
    # a.creat_new_table('HPA')
    # Builk.creat_new_table(HPA)

#current Function [db_insert] run time is 7.48














