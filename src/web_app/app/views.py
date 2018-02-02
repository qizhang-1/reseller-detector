from flask import request, redirect, render_template
from app import app
import pandas as pd
import os
import pymysql
import json


def connect_to_rdb():
    rs_conn = pymysql.connect(db=os.getenv('RS_DB'),
                              user=os.getenv('RS_USER'),
                              passwd=os.getenv('RS_PASSWORD'),
                              host=os.getenv('RS_HOST')
                              )
    return rs_conn

@app.route('/')
@app.route('/index', methods=['GET', 'POST'])
def index():
    error = None
    return render_template('index.html', error=error)

@app.route('/query')
def check_id():
    user_id = request.args.get('query')
    if len(user_id) < 2:
        return redirect('/index')
    rs_conn = connect_to_rdb()
    sql_com = "select count(1) from blacklist where user_id = '" + str(user_id) + "'"
    query_results = pd.read_sql_query(sql_com, rs_conn)

    if query_results is None or query_results.iloc[0][0] == 0:
        return redirect('/normal')
    else:
        return redirect('/blacklist')

@app.route('/blacklist')
def blacklist():
    error = None
    return render_template('blacklist.html', error=error)

@app.route('/normal')
def normal():
    error = None
    return render_template('normal.html', error=error)



@app.route('/ip')
def ip_page():
    rs_conn = connect_to_rdb()
    sql_com = "SELECT ipv4, COUNT(*) AS TIMES FROM blacklist \
               GROUP BY ipv4 \
               ORDER BY TIMES DESC \
               LIMIT 10"
    # executes sql query
    query_results = pd.read_sql_query(sql_com, rs_conn)
    results = "List of top 10 IP violations: <br>"
    for i in range(0, 10):
      results += query_results.iloc[i]['ipv4']
      results += " has "
      results += str(query_results.iloc[i]['TIMES'])
      results += " records."
      results += "<br>"
    return results

@app.route('/mac')
def mac_page():
    rs_conn = connect_to_rdb()
    sql_com = "SELECT en0, COUNT(*) AS TIMES FROM blacklist \
               GROUP BY en0 \
               ORDER BY TIMES DESC \
               LIMIT 10"
    # executes sql query
    query_results = pd.read_sql_query(sql_com, rs_conn)
    results = "List of top 10 MAC address violations: <br>"
    for i in range(0, 10):
        results += query_results.iloc[i]['en0']
        results += " has "
        results += str(query_results.iloc[i]['TIMES'])
        results += " records. "
        results += "<br>"
    return results

@app.route('/card')
def credit_card_page():
    rs_conn = connect_to_rdb()
    sql_com = "SELECT credit_card, COUNT(*) AS TIMES FROM blacklist \
               GROUP BY credit_card \
               ORDER BY TIMES DESC \
               LIMIT 10"
    # executes sql query
    query_results = pd.read_sql_query(sql_com, rs_conn)

    results = "List of top 10 credit card violations: <br>"
    for i in range(0, 10):
        results += query_results.iloc[i]['credit_card']
        results += " has "
        results += str(query_results.iloc[i]['TIMES'])
        results += " records."
        results += "<br>"
    return results

@app.route('/phone')
def cell_phone_page():
    rs_conn = connect_to_rdb()
    sql_com = "SELECT cell, COUNT(*) AS TIMES FROM blacklist \
               GROUP BY cell \
               ORDER BY TIMES DESC \
               LIMIT 10"
    # executes sql query
    query_results = pd.read_sql_query(sql_com, rs_conn)

    results = "List of top 10 cell phone violations: <br>"
    for i in range(0, 10):
        results += query_results.iloc[i]['cell']
        results += " has "
        results += str(query_results.iloc[i]['TIMES'])
        results += " records."
        results += "<br>"
    return results
