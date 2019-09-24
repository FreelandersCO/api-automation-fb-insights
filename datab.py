#import sqlalchemy
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, Date, Integer, exc, text
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, INTERVAL, JSON, JSONB, MACADDR, MONEY, NUMERIC, OID, REAL, SMALLINT, TEXT, TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, DATERANGE, TSRANGE, TSTZRANGE, TSVECTOR
import datetime , sys, json, os, argparse, logging, time


# define a class
class DatabaseOperation:
    def __init__(self):
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        user='sebas'
        password='sebas901029'
        db='havas_automation'
        host='localhost'
        port='5432'
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(user, password, host, port, db)

        # The return value of create_engine() is our connection object
        self.con = create_engine(url, client_encoding='utf8')

        # We then bind the connection to MetaData()
        self.meta = MetaData(bind=self.con, reflect=True)

        self.token_table = Table('page', self.meta, extend_existing=True)
        self.task_table = Table('task', self.meta, extend_existing=True)
        self.page_data = Table('page_data', self.meta, extend_existing=True)
        self.post_data = Table('post_data', self.meta, extend_existing=True)
        self.post_comment = Table('post_comment_data', self.meta, extend_existing=True)
        self.conversation = Table('conversation', self.meta, extend_existing=True)
        self.message = Table('message', self.meta, extend_existing=True)
      
    def get_table(self, table):
        switcher = {
            'token' : self.token_table,
            'task' : self.task_table,
            'page' : self.page_data,
            'post' : self.post_data,
            'comment' : self.post_comment,
            'conversation' : self.conversation,
            'message' : self.message,
        }
        return switcher.get(table, "Invalid Param")

    def insert(self, tableParam, data):
        table = self.get_table(tableParam)
        with self.con.connect() as conn:
            insert_statement = table.insert().values(data)
            conn.execute(insert_statement)
    
    def select(self, tableParam, fieldFilter = False, value = False):
        table = self.get_table(tableParam)
        with self.con.connect() as conn:
            if fieldFilter:
                select_statement = table.select().where(table.columns[fieldFilter] == value)
            else :
                select_statement = table.select()
            result_set = conn.execute(select_statement).fetchall()
            return result_set

    def update(self, tableParam, fieldFilter = False, value = False, data={}):
        table = self.get_table(tableParam)
        with self.con.connect() as conn:
            if fieldFilter:
                update_statement = table.update().where(table.columns[fieldFilter] == value).values(data)
                result_set = conn.execute(update_statement)
            else:
                result_set=0
            return result_set

    def delete(self, table , where_condition):
        query = 'DELETE FROM '+table+' WHERE '+where_condition+';'
        with self.con.connect() as conn:
            conn.execute(text(query))
