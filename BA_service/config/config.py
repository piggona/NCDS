ES_PATH = '127.0.0.1:5200/'
BASIC_QUERY = {
    "from": 0,
    "size": 10,
    "query": {
        "query_string": {
            "fields": ["title", "tags", "extend"],
            "query": ""
        }
    }
}
