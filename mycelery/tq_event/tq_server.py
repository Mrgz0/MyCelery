#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: gz

import logging


try:
    import pika
    from pika.adapters.tornado_connection import TornadoConnection
except ImportError:
    pika = None

try:
    import tornado
    import tornado.ioloop
except ImportError:
    tornado = None

logger = logging.getLogger('main.recieve_tornado')

class PikaConsumer(object):
    def __init__(self):
        if tornado is None:
            raise Exception('You must add tornado to your requirements!')
        if pika is None:
            raise Exception('You must add pika to your requirements!')

        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.connection = None
        self.channel = None
        self._delivery_tag = 0
        self.parameters = None
        self.queue_name = None
        self.on_task_end =None

    def connect(self):
        self.connection = TornadoConnection(self.parameters, on_open_callback=self.on_connected, stop_ioloop_on_close=False)
        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):
        logger.info('PikaConsumer: connected to RabbitMQ')
        self.connection.channel(on_open_callback=self.on_channel_declared)

    def on_channel_declared(self,channel):
        self.channel=channel
        self.channel.queue_declare(self.on_queue_declared, queue=self.queue_name)

    def on_queue_declared(self,method_frame):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.on_consume, queue=self.queue_name, no_ack=False)

    def on_closed(self, connection):
        logger.info('PikaConsumer: rabbit connection closed')
        self.connection.close()
        self.channel.close()
        self.ioloop.stop()

    def on_consume(self, channel, method, properties, body):
        try:
            if self.on_task_end:
                self.on_task_end(body)
        except Exception as e:
            logger.info('PikaConsumer: Error %r'%e)

        channel.basic_ack(delivery_tag = method.delivery_tag)

class PikaProducer(object):
    def __init__(self):
        if tornado is None:
            raise Exception('You must add tornado to your requirements!')
        if pika is None:
            raise Exception('You must add pika to your requirements!')

        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.connection = None
        self.channel = None
        self._delivery_tag = 0
        self.parameters = None
        self.queue_name = None

    def connect(self):
        self.connection = TornadoConnection(self.parameters, on_open_callback=self.on_connected, stop_ioloop_on_close=False)
        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):
        logger.info('PikaProducer: connected to RabbitMQ')
        self.connection.channel(on_open_callback=self.on_channel_declared)

    def on_channel_declared(self,channel):
        self.channel=channel
        self.channel.queue_declare(self.on_queue_declared, queue=self.queue_name)

    def on_queue_declared(self,method_frame):
        pass

    def publish(self,body):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=body)

    def on_closed(self, connection):
        logger.info('PikaProducer: rabbit connection closed')
        self.connection.close()
        self.channel.close()

import time
import asyncio
import pickle

class EventR:
    def __init__(self):
        self.t1 = time.time()
        self._is_finished = False
        self._result = None

    @property
    def is_finished(self):
        return self._is_finished

    @property
    def result(self):
        return self._result

    def set_result(self,result):
        self._is_finished=True
        self._result=result

class EventT:
    event_id = None
    event_body = None

    def to_bytes(self):
        return pickle.dumps(dict(event_id=self.event_id,event_body=self.event_body))


class EventContainer:
    container = {}

    @staticmethod
    def set(k,v):
        EventContainer.container.setdefault(k,v)

    @staticmethod
    def get(k):
        return EventContainer.container.get(k)

    @staticmethod
    def pop(k):
        return EventContainer.container.pop(k)

    @staticmethod
    def count():
        return len(EventContainer.container.keys())

class TQServer(object):
    _instance = None
    def __init__(self):
        self.amqp_url = 'amqp://guest:guest@127.0.0.1:5672/'
        self.task_queue = 'rq.task'
        self.result_queue = 'rq.result'

    def connect(self,amqp_url, task_queue=None, result_queue=None, event_container=None):
        self.consumer = PikaConsumer()
        self.producer = PikaProducer()

        if task_queue:
            self.task_queue = task_queue
        if result_queue:
            self.result_queue=result_queue

        if event_container:
            self.event_container = event_container
        else:
            self.event_container=EventContainer

        self.consumer.parameters = pika.URLParameters(amqp_url)
        self.consumer.queue_name = self.result_queue
        self.consumer.on_task_end = self.on_recv_event
        self.producer.parameters = pika.URLParameters(amqp_url)
        self.producer.queue_name = self.task_queue
        self.consumer.connect()
        self.producer.connect()

    def get_event(self,event_id):
        return self.event_container.get(event_id)

    def remove_event(self,event_id):
        return self.event_container.pop(event_id)

    def register_event(self,body):
        import uuid
        event_id =uuid.uuid1().hex
        event_result = EventR()
        self.event_container.set(event_id, event_result)

        event_t = EventT()
        event_t.event_id = event_id
        event_t.event_body = body
        return event_t

    async def on_send_event(self,event_str):
        self.producer.publish(event_str)

    async def send_event(self,body):
        event_t = self.register_event(body)
        await self.on_send_event(event_t.to_bytes())
        logger.info("Sended %r" % event_t.__dict__)
        return event_t.event_id

    def get_expire_time(self,timeout):
        import time
        default_timeout = 60
        if timeout and timeout>=1:
            return time.time()+timeout
        return time.time()+default_timeout

    def check_expire(self,expire_time):
        import time
        return time.time()>expire_time

    async def delay(self):
        interval=0.05
        await asyncio.sleep(interval)

    async def do_async_task(self,body, timeout=None):
        event_id = await self.send_event(body)
        expire_time = self.get_expire_time(timeout)
        while True:
            event = self.get_event(event_id)
            if event is None:
                return None
            if event.is_finished:
                result = event.result
                self.remove_event(event_id)
                return result
            if self.check_expire(expire_time):
                self.remove_event(event_id)
                raise Exception('EventR timeout')
            await self.delay()

    def on_recv_event(self, body):
        try:
            import pickle
            event_t = pickle.loads(body)
            event = self.get_event(event_t.get("event_id"))
            if event:
                event.set_result(event_t.get("event_body"))
            logger.info("Recved %r" % event_t)
        except Exception as e:
            pass
        # 丢弃

_tq_server = TQServer()

def connect(amqp_url, task_queue=None, result_queue=None, event_container=None):
    _tq_server.connect(amqp_url, task_queue, result_queue, event_container)

async def do_async_task( body, timeout=None):
    return await _tq_server.do_async_task(body, timeout)