from flask import Flask,request,render_template
from urllib.parse import urlencode,quote,unquote
from BA_service.es.es_op import es_search
from BA_service.config.config import BASIC_QUERY
from flask_cors import *
app = Flask(__name__)
CORS(app, supports_credentials=True)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/_search',methods=["get","post"])
def get_search():
    if request.method == 'GET':
        query = request.args.get('query')
        print(query)
        search_item = es_search(BASIC_QUERY)
        search_item.build_search_query(query,0,10)
        result = search_item.search_for_all()
        return result
    elif request.method == 'POST':
        query = request.form["query"]
        from_page = request.form["from"]
        page_size = request.form["size"]
        print(query)
        search_item = es_search(BASIC_QUERY)
        search_item.build_search_query(query,from_page,page_size)
        result = search_item.search_for_all()
        return result

@app.route('/api/_search',methods=["POST"])
def ajax_search():
    query = request.form.get('query')
    from_page = request.form.get('from')
    page_size = request.form.get('size')
    print(query)
    search_item = es_search(BASIC_QUERY)
    search_item.build_search_query(query,from_page,page_size)
    result = search_item.search_for_all()
    return result


    
 
if __name__ == '__main__':
    app.run(host='0.0.0.0')