import logging
import os
from aiohttp import web
import clickhouse_connect
import jwt
from typing import Callable, Awaitable
import orjson
import datetime


@web.middleware
async def mw_cors(request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]):
    headers = {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Authorization, Content-Type'
    }
    if request.method == 'OPTIONS':
        return web.Response(status=200, headers=headers)
    resp = await handler(request)
    return resp

class ReportsHandler(web.View):
    def get_user_id_from_jwt(self):
        token = self.request.headers.get('Authorization')
        dtoken = jwt.decode(token.replace('Bearer ', ''), options={"verify_signature": False})
        return dtoken["sub"]

    async def get(self) -> web.Response:
        headers = {
            'Access-Control-Allow-Origin': 'http://localhost:3000',
            'Access-Control-Allow-Headers': 'Authorization, Content-Type'
        }
        user_id = self.get_user_id_from_jwt()
        client: clickhouse_connect.driver.Client = app["click_conn"]
        data = client.query(f"select * from reports where user_id = '{user_id}'")
        logging.info(f"{data = }")
        logging.info(f"{data.result_rows = }")
        resp = orjson.dumps(data.result_rows)
        return web.Response(body=resp, headers=headers)


def add_routes(app: web.Application):
    routes = [
        ('*', r'/reports', ReportsHandler),
    ]

    for method, path, handler in routes:
        app.router.add_route(method=method, path=path, handler=handler)


if __name__ == "__main__":
    app = web.Application(middlewares=[mw_cors])
    logging.basicConfig(level=logging.INFO)
    client = clickhouse_connect.get_client(
        host="clickhouse",
        username="admin",
        password=os.environ.get("CLICK_PASS"),
    )
    # client = Client(f'clickhouse://admin:{os.environ.get("CLICK_PASS")}@localhost')
    app["click_conn"] = client
    add_routes(app=app)
    web.run_app(app, port=8000)
