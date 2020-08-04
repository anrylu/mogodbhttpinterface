import json
from flask import Flask, request
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure, AutoReconnect

app = Flask(__name__)
mongo_connections = {}

@app.route('/_connect', methods=['POST'])
def connect():
    name = request.form.get('name', 'default')
    server = request.form.get('server', 'localhost:27017')
    app.logger.info('connect to %s (%s)', server, name)
    try:
        client = MongoClient('mongodb://' + server)
        mongo_connections[name] = client
        return {
            "ok": 1,
            "server": server,
            "name": name
        }
    except Exception:
        return {
            "ok": 0,
            "errmsg": "could not connect",
            "server": server,
            "name": name}


@app.route('/<dbname>/_authenticate', methods=['POST'])
def authenticate(dbname):
    name = request.form.get('name', 'default')
    username = request.form.get('username')
    password = request.form.get('password')

    # get connection
    client = mongo_connections.get(name)
    if client is None:
        return {
            "ok": 0,
            "errmsg": "couldn\'t get connection to mongo"
        }

    try:
        app.logger.info(
            'authenticate %s with %s (%s)',
            dbname, username, password)
        client[dbname].authenticate(
            username,
            password,
            source='admin',
            mechanism='SCRAM-SHA-1'
        )
        return {
            "ok": 1
        }
    except Exception:
        app.logger.exception("authenticate failed")
        return {
            "ok": 0,
            "errmsg": "authentication failed"
        }


@app.route('/<dbname>/_cmd', methods=['POST'])
def cmd(dbname):
    pass


@app.route('/<dbname>/<collname>/_insert', methods=['POST'])
def insert(dbname, collname):
    name = request.form.get('name', 'default')

    # get connection
    client = mongo_connections.get(name)
    if client is None:
        return {
            "ok": 0,
            "errmsg": "couldn\'t get connection to mongo"
        }

    # get docs to insert
    docs = json.loads(request.form.get('docs'))
    if not docs:
        return {
            "ok": 0,
            "errmsg": "missing docs"
        }

    try:
        app.logger.exception("insert %s", docs)
        client[dbname][collname].insert(docs)
        return {
            "ok": 1
        }
    except Exception:
        app.logger.exception("insert failed")
        return {
            "ok": 0,
            "errmsg": "insert failed"
        }


def __output_results(cursor):
    """
    Iterate through the next batch
    """
    results = []

    try:
        while True:
            result = cursor.next()
            result['_id'] = str(result['_id'])
            results.append(result)
    except AutoReconnect:
        return {"ok": 0, "errmsg": "auto reconnecting, please try again"}
    except OperationFailure as of:
        return {"ok": 0, "errmsg": "%s" % of}
    except StopIteration:
        pass

    return {
        "results": results,
        "ok" : 1
    }


@app.route('/<dbname>/<collname>/_find', methods=['GET'])
def find(dbname, collname):
    name = request.form.get('name', 'default')

    # get connection
    client = mongo_connections.get(name)
    if client is None:
        return {
            "ok": 0,
            "errmsg": "couldn\'t get connection to mongo"
        }

    # get criteria
    criteria = request.args.get('criteria')
    app.logger.info("find by criteria %s", criteria)
    if criteria:
        criteria = json.loads(criteria)

    # get fields
    fields = request.args.get('fields')
    if fields:
        fields = json.loads(fields)

    # get limit
    limit = int(request.args.get('limit', 0))

    # get skip
    skip = int(request.args.get('skip', 0))

    # get sort
    sort_to_use = []
    sort = request.args.get('sort')
    if sort:
        sort = json.loads(sort)
        for sort_key in sort:
            if sort[sort_key] == -1:
                sort_to_use.append([sort_key, DESCENDING])
            else:
                sort_to_use.append([sort_key, ASCENDING])

    try:
        app.logger.info("find by criteria %s", criteria)
        cursor = client[dbname][collname].find(
            criteria, fields, skip, limit, sort=sort_to_use)
        setattr(cursor, "id", id)
        return __output_results(cursor)
    except Exception:
        app.logger.exception("find failed")
        return {
            "ok": 0,
            "errmsg": "find failed"
        }


@app.route('/<dbname>/<collname>/_update', methods=['POST'])
def update(dbname, collname):
    name = request.form.get('name', 'default')

    # get connection
    client = mongo_connections.get(name)
    if client is None:
        return {
            "ok": 0,
            "errmsg": "couldn\'t get connection to mongo"
        }

    # get criteria
    criteria = request.form.get('criteria')
    if criteria:
        criteria = json.loads(criteria)

    # get newobj
    newobj = request.form.get('newobj')
    if newobj:
        newobj = json.loads(newobj)
    if not newobj:
        return {"ok": 0, "errmsg": "missing newobj"}

    # get upsert
    upsert = request.form.get('upsert', False)
    if upsert:
        upsert = bool(upsert)

    # get multi
    multi = request.form.get('multi', False)
    if multi:
        multi = bool(multi)

    try:
        app.logger.info("update by criteria %s", criteria)
        if multi:
            client[dbname][collname].update_many(
                criteria,
                newobj,
                upsert)
        else:
            client[dbname][collname].update_one(
                criteria,
                newobj,
                upsert)
        return {
            "ok": 1
        }
    except Exception:
        app.logger.exception("update failed")
        return {
            "ok": 0,
            "errmsg": "update failed"
        }


@app.route('/<dbname>/<collname>/_remove', methods=['POST'])
def remove(dbname, collname):
    name = request.form.get('name', 'default')

    # get connection
    client = mongo_connections.get(name)
    if client is None:
        return {
            "ok": 0,
            "errmsg": "couldn\'t get connection to mongo"
        }

    # get criteria
    criteria = request.form.get('criteria')
    if criteria:
        criteria = json.loads(criteria)

    try:
        app.logger.info("remove by criteria %s", criteria)
        client[dbname][collname].delete_many(criteria)
        return {
            "ok": 1
        }
    except Exception:
        app.logger.exception("remove failed")
        return {
            "ok": 0,
            "errmsg": "remove failed"
        }

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=27080)
