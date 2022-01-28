import os
import sys
import cx_Oracle
import json
import itertools
from flask import Flask
from flask import jsonify


app = Flask(__name__)

def init_session(connection, requestedTag_ignored):
    cursor = connection.cursor()
    cursor.execute("""
        ALTER SESSION SET
          TIME_ZONE = 'UTC'
          NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI'""")

def start_pool():

    # Generally a fixed-size pool is recommended, i.e. pool_min=pool_max.
    # Here the pool contains 4 connections, which is fine for 4 concurrent
    # users.
    #
    # The "get mode" is chosen so that if all connections are already in use, any
    # subsequent acquire() will wait for one to become available.

    pool_min = 4
    pool_max = 4
    pool_inc = 0
    pool_gmd = cx_Oracle.SPOOL_ATTRVAL_WAIT

    #print("Connecting to", os.environ.get("PYTHON_CONNECTSTRING"))

    dsnStr = cx_Oracle.makedsn("localhost", "1521", "ORCLCDB")

    pool = cx_Oracle.SessionPool(user="DWBI_SPS",
                                 password="DWBISPS",
                                 dsn=dsnStr,
                                 min=pool_min,
                                 max=pool_max,
                                 increment=pool_inc,
                                 threaded=True,
                                 getmode=pool_gmd,
                                 sessionCallback=init_session)

    return pool


@app.route('/get_stg_table', methods = ['GET'])
def get_stg_table():
    con = pool.acquire()
    cursor = con.cursor()
    sql = """SELECT TABLE_NAME FROM DWBI_SPS.PS_ALL_TABLES WHERE upper(owner) = 'DWBI_STG' """
    cursor.execute(sql)


    result = cursor.fetchall()

    result = list(result[i][0] for i in range(len(result)))

    response = app.response_class(
        response=json.dumps(dict({"list_table" : result})),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/table/<string:table_name>', methods = ['GET'])
def get_table_column(table_name):
    print(table_name)
    connection = pool.acquire()
    cursor = connection.cursor()
    sql = """
        SELECT 
            distinct column_id,COLUMN_NAME 
        FROM 
            DWBI_SPS.PS_ALL_TAB_COLUMNS 
        WHERE 
            OWNER LIKE 'DWBI_STG'
            AND upper(TABLE_NAME) LIKE :table_name 
        order by column_id
        """
    cursor.execute(sql, [table_name])
    result = cursor.fetchall()

    result = list({result[i][0]:result[i][1]} for i in range(len(result)))
    print(result)
    response = app.response_class(
        response=json.dumps({table_name:result}),
        status=200,
        mimetype='application/json'
    )
    return response

'''
@app.route('/get_stg_table', methods = ['GET'])
def get_stg_table():
    con = pool.acquire()
    cursor = con.cursor()
    sql = """SELECT COLUMN_NAME FROM DWBI_SPS.PS_ALL_TAB_COLUMNS WHERE OWNER LIKE 'DWBI_STG'
AND upper(TABLE_NAME) LIKE 'SIPU_OMNI_OUTFLOW_CYCLICAL_INDEX_V' """
    cursor.execute(sql)
    


    result = cursor.fetchall()

    result = list(result[i][0] for i in range(len(result)))

    response = app.response_class(
        response=json.dumps(dict({"list_table" : result})),
        status=200,
        mimetype='application/json'
    )
    return response
'''


if __name__ == '__main__':
    pool = start_pool()
    app.run(debug = True,port=int(os.environ.get('PORT', '8080')))