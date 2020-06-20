#!/usr/bin/env python
import logging
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
log = logging.getLogger()

CONNECTION_STRING = 'postgres+psycopg2://@localhost:5432/Andy_iMac'
DEFAULT_SCHEMA = 'public'
DEFAULT_TABLE = '???'
ALTERNATE_SCHEMA = 'pg_catalog'
ALTERNATE_TABLE = 'pg_statistic'
ALTERNATE_VIEW = 'pg_settings'
FULL_QUERY = """
SELECT *
FROM pg_catalog.pg_type
;
"""

# connect to local pg instnace
log.info(f"Getting database connection")
db_conn = create_engine(CONNECTION_STRING)
log.info(f"Got it, connection is {db_conn}")
log.info(f"-------------------------------")

# try using read_sql_query method
log.info(f"Getting data into pandas using the query")
df = pd.read_sql_query(FULL_QUERY, db_conn)
log.info(f"Data is {df.head()}")
log.info(f"-------------------------------")

# try using read_sql_table method
log.info(f"Getting data into pandas using the read_sql_table")
df = pd.read_sql_table(ALTERNATE_TABLE, db_conn, schema=ALTERNATE_SCHEMA)
log.info(f"Data is {df.head()}")
log.info(f"-------------------------------")

# now try using the read_sql wrapper
log.info(f"Getting data into pandas using the read_sql wrapper")
df = pd.read_sql(ALTERNATE_TABLE, db_conn, schema=ALTERNATE_SCHEMA)
log.info(f"Data is {df.head()}")
log.info(f"-------------------------------")
