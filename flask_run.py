
import pymysql
import time
from datetime import datetime
from flask import Flask, request, render_template
from urllib.parse import urlencode, quote, unquote
from BA_service.es.es_op import es_search
from BA_service.config.config import BASIC_QUERY
from flask_cors import *
app = Flask(__name__)
CORS(app, supports_credentials=True)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/_search', methods=["get", "post"])
def get_search():
    if request.method == 'GET':
        query = request.args.get('query')
        print(query)
        search_item = es_search(BASIC_QUERY)
        search_item.build_search_query(query, 0, 10)
        result = search_item.search_for_all()
        return result
    elif request.method == 'POST':
        query = request.form["query"]
        from_page = request.form["from"]
        page_size = request.form["size"]
        print(query)
        search_item = es_search(BASIC_QUERY)
        search_item.build_search_query(query, from_page, page_size)
        result = search_item.search_for_all()
        return result


@app.route('/api/_search', methods=["POST"])
def ajax_search():
    query = request.form.get('query')
    from_page = request.form.get('from')
    page_size = request.form.get('size')
    print(query)
    search_item = es_search(BASIC_QUERY)
    search_item.build_search_query(query, from_page, page_size)
    result = search_item.search_for_all()
    return result


@app.route('/api/_push', methods=["POST"])
def push_data():
    item_id = request.form.get('id')
    title = request.form.get('title')
    url = request.form.get('url')
    push_time = datetime.now()
    create_time = int(time.time())
    update_time = int(time.time())
    conn = pymysql.connect(host='rm-2zeg7277v9fkmj3bi.mysql.rds.aliyuncs.com',
                           port=3306, user="information", db="infomation", passwd="Infor0110")
    cursor = conn.cursor()
    query = "INSERT INTO mine_umeng_push (item_id,title,url,type,push_time,status,created_at,updated_at) VALUES ({0},'{1}','{2}',{3},'{4}',{5},{6},{7})".format(
        item_id, title, url, 3, push_time,2, 'NOW()', 'NOW()')
    print(query)
    try:
        cursor.execute(query)
    except Exception as e:
        print(e)
    conn.commit()
    cursor.close()
    conn.close()
    return "OK"
    


if __name__ == '__main__':
    app.run(host='0.0.0.0')
