import os
import sqlite3 #import cx_Oracle
import pandas as pd
from . import log

class networkcon(object):
    """
    List of functions to call queries of specific type from relative path
    'query_path'. Note that most of the queries to be used by functions below
    must have specific type of input paramteres and output columns.

    i.e.
    it must be made sure that a query returns at least the row that is expected
    in index column
    """

    con = None

    # queries_folder is the name of the folder wrt this class where we keep queries
    # that are to be executed by functions of this class
    queries_folder = 'queries'

    def __init__(self, dbname): # def __init__(self, conn_string):
        """Oracle or similar dbs may need connection string"""

        self.dbname = dbname # self.conn_string = conn_string
        self.query_path = os.path.join(os.path.dirname(__file__)
            , self.queries_folder)

    def connect(self):
        self.con = sqlite3.connect(self.dbname, detect_types=sqlite3.PARSE_DECLTYPES)
        # self.con = cx_Oracle.connect(self.conn_string, encoding='UTF-16', nencoding='UTF-16')
        log.log_to_scr(__file__, "conected to db...")
        log.log_to_scr(__file__, "fetching queries from " + self.query_path )

    def disconnect(self):
        if self.con:
            self.con.close()
            log.log_to_scr(__file__, str(self.dbname)+" is closed.")
        else:
            log.log_to_scr(__file__, "nothing to close")

    def read_query(self, full_query_path):
        """simply read queries"""
        log.log_to_scr("networkcon", "running " + str(full_query_path))
        with open(full_query_path, "r") as query:
            return query.read()

    def get_simple(self, query):
        """returns a dataframe in its most basic form"""
        return pd.read_sql(self.read_query(self.query_path + "\\" + query)
            , con=self.con)

    def get_custom(self, query, params, index_col):
        """returns a dataframe with optional parameters for query and index
        columns. This is the most generic way of running a query and get result
        in a dataframe"""
        return pd.read_sql( self.read_query(self.query_path + "\\" + query)
                            , con=self.con
                            , params=params
                            , index_col=index_col)

    def get_dtime(self, param1, param2  , query="query_dtim.sql"
                                                    , index_col='DATETIME'):
        return pd.read_sql( self.read_query(self.query_path + "\\" + query),
                            con=self.con,
                            params={'FILTER_1':param1, 'FILTER_2':param2},
                            index_col=index_col)

    def get_cat(self, param, query="query_cat.sql", index_col='CATEGORY_1'):
        return pd.read_sql( self.read_query(self.query_path + "\\" + query),
                            con=self.con,
                            params={'FILTER':param},
                            index_col=index_col)

    def get_dtim_cat(self, param1, param2, query="query_dtim_cat.sql"
                                    , index_col=['DATETIME','CATEGORY_1']):
        return pd.read_sql( self.read_query(self.query_path + "\\" + query),
                                con=self.con,
                                params={'FILTER_1':param1, 'FILTER_2':param2},
                                index_col=index_col)
