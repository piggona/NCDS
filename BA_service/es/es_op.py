import requests
import json
import elasticsearch

res = requests.get('http://localhost:5200/mifeng_article/mifeng/_search')
print(res.content)


def search_for_all(query_body):
    es = elasticsearch.Elasticsearch([{"host": 'localhost','port': 5200}])
    try:
        res = es.search(index='mifeng_article',body = query_body)
        RES = json.dumps(res,indent=2)
        print(RES)
    except elasticsearch.exceptions.NotFoundError:
        print("not found %".format(query_body))