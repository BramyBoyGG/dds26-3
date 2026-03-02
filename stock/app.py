import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from lua_scripts import LUA_SUBTRACT_STOCK, LUA_ADD_STOCK


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)

# Register Lua scripts with Redis
subtract_script = db.register_script(LUA_SUBTRACT_STOCK)
add_script = db.register_script(LUA_ADD_STOCK)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    amount = int(amount)
    try:
        new_stock = add_script(keys=[item_id], args=[amount])
        if new_stock == 0:
            abort(400, f"Item: {item_id} not found!")
        return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    amount = int(amount)
    try:
        new_stock = subtract_script(keys=[item_id], args=[amount])
        if new_stock == 0:
            abort(400, f"Item: {item_id} not found!")
        elif new_stock == -1:
            abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
        app.logger.debug(f"Item: {item_id} stock updated to: {new_stock}")
        return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
