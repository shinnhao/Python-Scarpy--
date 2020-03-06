# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exporters import JsonLinesItemExporter
from fang.items import NewHouseItem,ESFHouseItem
import pymysql
class FangPipeline(object):
    def __init__(self):
        dbparams = {
            'host': '127.0.0.1',
            'user':'root',
            'password':'123456',
            'database':'gdfdc',
            'charset':'utf8'
        }
#'post':3306,
        self.conn = pymysql.connect(**dbparams)
        self.cursor =self.conn.cursor()
        self._sql =None
        self._esfsql=None
    '''
    
        self.newhouse_fp = open('newhouse.json',"wb")
        self.esfhouse_fp = open('esfhouse.json', "wb")
        self.newhouse_exporter = JsonLinesItemExporter(
            self.newhouse_fp,
            ensure_ascii=False
        )
        self.esfhouse_exporter = JsonLinesItemExporter(
            self.esfhouse_fp,
            ensure_ascii=False
        )
    '''

    def process_item(self, item, spider):
        if isinstance(item, NewHouseItem):
            self.cursor.execute(self.sql,(item['province'],item['city'],item['name'],item['price'],item['rooms'],
                                      item['area'],item['address'],item['district'],item['sale'],item['origin_url']))
            self.conn.commit()
        elif isinstance(item, ESFHouseItem):
            self.cursor.execute(self.esfsql, (item['province'], item['city'], item['name'], item['rooms'], item['floor'],
                                           item['toward'], item['year'], item['address'], item['area'],
                                           item['price'], item['unit'], item['origin_url']))
        '''
        if isinstance(item, NewHouseItem):
            self.newhouse_exporter.export_item(item)
        elif isinstance(item, ESFHouseItem):
            self.esfhouse_exporter.export_item(item)
        '''

        return item

    @property
    def sql(self):
        if not self._sql:
            self._sql = """
            insert into newhouse(id,province,city,name,price,rooms,area,address,district,sale,
            origin_url) values(null,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            return self._sql
        return self._sql

    @property
    def esfsql(self):
        if not self._esfsql:
            self._esfsql = """
                insert into esf(id,province,city,name,rooms,floor,toward,year,address,area,
                price,unit,origin_url) values(null,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
            return self._esfsql
        return self._esfsql

    '''

    def close_spider(self,spider):
        self.newhouse_fp.close()
        self.esfhouse_fp.close()

    '''