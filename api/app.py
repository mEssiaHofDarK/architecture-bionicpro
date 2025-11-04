import logging
from aiohttp import web
from clickhouse_driver import Client


class ReportsHandler(web.View):
    async def get(self):
        client: Client = app["click_conn"]
        data = client.execute("SELECT * FROM reports where user_id = %(user_id)s", {"user_id": self.request.query["user_id"]})
        return web.Response(body=data)


def add_routes(app: web.Application):
    routes = [
        ('*', r'/reports', ReportsHandler),
    ]

    for method, path, handler in routes:
        app.router.add_route(method=method, path=path, handler=handler)


if __name__ == "__main__":
    app = web.Application()
    logging.basicConfig(level=logging.INFO)
    app["click_conn"] = Client("clickhouse:8123")
    add_routes(app=app)
    web.run_app(app, port=8000)
