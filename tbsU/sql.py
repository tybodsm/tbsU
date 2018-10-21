#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Provides functions for interacting with sql databases easily and getting ETLs up and running supa 
fast! For right now, only supports psql.
'''

from datetime import datetime
from io import StringIO
import os

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

#####################################
# Helper Functions                  #
#####################################

def _add_cols(df, needed_cols):
    return dict((i, np.nan) for i in needed_cols if i not in df.columns)

def _dedupe_ignore(df, dupe_keys, dedupe_range, table_name, engine):
    keylist_sql = ', '.join(dupe_keys)
    range_str = ''
    if len(dedupe_range) != 0:
        sql_range_list = []
        for k, v in dedupe_range.items():
            sql_range_list.append(f'{k} >= {v[0]} and {k} <= {v[1]}')
        range_str = ' and ' + ' and '.join(sql_range_list)

    sql = '''
        select distinct {keylist_sql}
            from {table_name}
            where 1 = 1
                {range_str} 
    '''.format(
        range_str=range_str,
        table_name=table_name,
        keylist_sql=keylist_sql
    )

    check = psql_load(sql, engine=engine)

    # The index naming may come back to bite me at some point...
    keep_inds = (df[dupe_keys]
                 .reset_index()
                 .merge(check, how='left', indicator=True)
                 .set_index('index')
                 ['_merge']
                 == 'left_only'
                 )

    return df[keep_inds]

#####################################
# Main Functions                    #
#####################################

def set_tbsU_engine(dw_host, dw_user, dw_name):
    global dw_engine
    args = {'user': dw_user, 'host': dw_host, 'db': dw_name}
    dw_engine = create_engine('postgresql://{user}@{host}/{db}'.format(**args))

def sql_time(time, offset=0, offset_unit='days'):
    if isinstance(time, str) or isinstance(time, datetime):
        t = pd.to_datetime(time) + pd.Timedelta(**{offset_unit: offset})
        return t.strftime('%Y-%m-%d %H:%M:%S')
    raise ValueError('Only strings or datetime objects are supported')

def psql_load(code, engine=None, db='', host='', user=''):
    ''' Allows a flexible draw from a database into pandas directly using a select statement '''
    if engine is None:
        engine = dw_engine
    if (not (db and host and user)) and not engine:
        raise ValueError('SQL connection must be specified by either an engine or connection details')

    if engine is None:
        args = {'user': user, 'host': host, 'db': db}
        engine = create_engine('postgresql://{user}@{host}/{db}'.format(**args))
    out = pd.read_sql_query(code, con=engine)
    return out

def df_to_pg(df, table_name, engine=None, conflict='fail', dupes='include', dupe_keys=[],
             dedupe_side='server', dedupe_range={}, grant=None, grant_types='SELECT'):
    ''' 
    Allows for creating, appending onto, or replacing tables in sql with pandas dataframes. Performs
    this in a more efficient way than the standard pd.to_sql by writing from buffer rather than 
    using a mass of select statements.

    Parameters
    ----------
    df : pandas dataframe
        The dataframe to load into Postgres
    table_name : string
        table name to use in the database
    engine : sql_alchemy engine, optional
        Engine defining the connection to the database. Uses tbsu environment variables by default.
    conflict : {'fail', 'append', 'replace'}, optional
        Engine defining the connection to the database
        'fail' causes the operation to fail if the table is found.
        'append' adds the observations onto the existing table.
        'replace' replaces the postgres table with the data in the dataframe.
    dupes : {'include', 'update', 'ignore'}, optional
        Engine defining the connection to the database. If not 'include', 'dupe_keys' must be 
        provided for identifying duplicates.
        'include': no deduping
        'update': NOT YET IMPLEMENTED replaces duplicates in postgres with versions in the dataframe
        'ignore': only uploads rows for which the primary key values don't exist already in postgres
    dupe_keys : array-like, optional
        Column names of primary keys for the table and dataframe.
    dedupe_side : {'server', 'client'}, optional 
        NOT YET OPERATIONAL
        Specifies whether the dedupe is done by loading
    dedupe_range : dict-like, optional
        Specify a dict of format {column: (min, max)} for narrowing down the range of potential 
        duplicates. Speeds up client-side deduping.
    grant : string or array-like
        Specify a permission group, or list of permission groups, to grant access to
    grant_types : string or array-like, optional (default 'SELECT')
        Specify the types of access to grant (e.g 'UPDATE')
    '''

    if engine is None:
        engine = dw_engine

    if dupes != 'include' and len(dupe_keys) == 0:
        raise ValueError("Cannot dedupe: no dupe_keys provided")

    if conflict not in ['replace', 'append', 'fail']:
        raise ValueError("'conflict' must be one of ['replace', 'append', 'fail']")

    if engine is None:
        raise ValueError('No tbsu default engine. Please provide an sql_alchemy engine.')

    if isinstance(grant, str):
        grant = [grant]

    if isinstance(grant_types, str):
        grant_types = [grant_types]

    raw = engine.raw_connection()
    curs = raw.cursor()

    try:
        # Handle if table already exists
        if engine.dialect.has_table(engine, table_name):
            if conflict == 'fail':
                    raise AssertionError("Table already exits")

            elif conflict == 'drop':
                curs.execute(f"DROP TABLE IF EXISTS {table_name} cascade;")

            elif conflict == 'append':

                # Account for differences in columns
                server_cols = (
                    pd.read_sql_query(f'SELECT * FROM {table_name} limit 1;', con=engine)
                    .columns
                )

                if (set(df.columns) <= set(server_cols)):
                    df = df.assign(**_add_cols(df, server_cols))

                else: 
                    raise ValueError('Columns found that do not exist in existing sql table.'
                                     + 'Please regenerate table with full set.')

                # Dedupe
                if dupes == 'ignore':
                    df = _dedupe_ignore(df, dupe_keys, dedupe_range, table_name, engine)

                if dupes == 'update':
                    raise ValueError("Sorry, this isn't implemented yet :/")
                    
        added_obs = len(df.index)
        if added_obs == 0:
            print('No observations in df.')
            return
                    
        #prep data for load
        data = StringIO()
        df.to_csv(data, header=False, index=False, sep='\u0005')
        data.seek(0)

        #create table with correct types
        if (not engine.dialect.has_table(engine, table_name)) or (conflict == 'drop'):
            empty_table = pd.io.sql.get_schema(df, table_name, con=engine)
            empty_table = empty_table.replace('"', '')
            curs.execute(empty_table)
    
        #populate the table
        cols = ', '.join(df.columns)
        sql_code = f"COPY {table_name}({cols}) FROM STDIN WITH CSV DELIMITER E'\x05';"
        curs.copy_expert(sql=sql_code, file=data)
        curs.connection.commit()
        
        #grant view permissions to analytics folks
        if grant:
            curs.execute(f"GRANT {', '.join(grant_types)} ON {table_name} TO {', '.join(grant)} ;")
    
        curs.connection.commit()
        return added_obs

    except:
        raw.rollback()
        raise

    finally:
        raw.close() 

#####################################
# Initialize                        #
#####################################

dw_host = os.getenv('TBSU_DW_HOST')
dw_user = os.getenv('TBSU_DW_USER', os.getenv('USER'))
dw_name = os.getenv('TBSU_DW_NAME')

dw_engine = None

if dw_host and dw_user and dw_name:
    set_tbsU_engine(dw_host, dw_user, dw_name)



