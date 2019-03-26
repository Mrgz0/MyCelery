#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: gz


import logging
import tornado.web
import tornado.gen
import tornado
import tornado.ioloop

logger = logging.getLogger('main.recieve_tornado')


class MainHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self, *args, **kwargs):
        from tq_event import tq_server
        result = yield tq_server.do_async_task({'action': "test", "param": {"a": 1, "b": 2}})
        print('Recv async tast result', result)


urls = [[r'/.*', MainHandler], ]

amqp_url = "amqp://t:123456@127.0.0.1:5672/"

class Application(tornado.web.Application):
    def __init__(self):
        from tq_event import tq_server
        tq_server.connect(amqp_url)
        tornado.web.Application.__init__(self, handlers=urls)


import tornado.httpserver
import tornado.ioloop

app = Application()

import tornado.options

tornado.options.parse_command_line()
http_server = tornado.httpserver.HTTPServer(app)
http_server.listen(8905)
print('Server is running on port %s.')
tornado.ioloop.IOLoop().instance().start()
