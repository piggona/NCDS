from flask import Flask,request,render_template
from urllib.parse import urlencode,quote,unquote
from BA_service.es.es_op import es_search
from BA_service.config.config import BASIC_QUERY
app = Flask(__name__)
 
@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/_search',methods=["get"])
def get_search():
    query = request.args.get('query')
    print(query)
    search_item = es_search(BASIC_QUERY)
    search_item.build_search_query(query,0,10)
    search_item.search_for_all()
    
 
if __name__ == '__main__':
    app.run()