import logging
from aiohttp import web

# from db.get_pool import pg_context


class ReportsHandler(web.View):
    async def get(self):
        return web.Response(text="i'am report")


def add_routes(app: web.Application):
    routes = [
        ('*', r'/reports', ReportsHandler),
    ]

    for method, path, handler in routes:
        app.router.add_route(method=method, path=path, handler=handler)


if __name__ == "__main__":
    app = web.Application()
    logging.basicConfig(level=logging.INFO)
    # app.cleanup_ctx.append(pg_context)
    add_routes(app=app)
    web.run_app(app, port=8000)
