#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: gz



class TQClient(object):
    def __init__(self):
        self.amqp_url = 'amqp://guest:guest@127.0.0.1:5672/'
        self.parameters = None
        self.task_queue = 'rq.task'
        self.result_queue = 'rq.result'
        self.callback = None

    def connect(self, amqp_url, task_queue=None, result_queue=None, callback=None):
        if amqp_url:
            self.amqp_url = amqp_url
        if task_queue:
            self.task_queue = task_queue
        if result_queue:
            self.result_queue = result_queue
        if callback:
            self.callback = callback

        import pika
        self.parameters = pika.URLParameters(self.amqp_url)
        self.publisher_connection = pika.BlockingConnection(self.parameters)
        self.publisher_channel = self.publisher_connection.channel()
        self.publisher_channel.queue_declare(queue=self.result_queue)

        self.consumer_connection = pika.BlockingConnection(self.parameters)
        self.consumer_channel = self.consumer_connection.channel()
        self.consumer_channel.queue_declare(queue=self.task_queue)

    def _publish(self, s):
        import pika
        try:
            self.publisher_channel.basic_publish(exchange='', routing_key=self.result_queue, body=s)
        except pika.exceptions.ConnectionClosed as e:
            self.publisher_connection = pika.BlockingConnection(self.parameters)
            self.publisher_channel = self.publisher_connection.channel()
            self.publisher_channel.basic_publish(exchange='', routing_key=self.result_queue, body=s)


    def _do_callback(self, ch, method, properties, body):
        if self.callback is not None:
            import pickle
            event_t = pickle.loads(body)
            event_id = event_t.get('event_id')
            event_body = event_t.get('event_body')
            result = self.callback(event_body)
            event_r = {"event_id": event_id, "event_body": result}
            event_bytes = pickle.dumps(event_r)
            self._publish(event_bytes)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consume(self):
        self.consumer_channel.basic_qos(prefetch_count=1)
        self.consumer_channel.basic_consume(self._do_callback, queue=self.task_queue, no_ack=False)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.consumer_channel.start_consuming()

_tq_client = TQClient()


def connect( amqp_url, task_queue=None, result_queue=None, callback=None):
    _tq_client.connect(amqp_url,task_queue,result_queue,callback)

def start_consume():
    _tq_client.start_consume()