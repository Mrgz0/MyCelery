#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: gz


amqp_url = "amqp://t:123456@127.0.0.1:5672/"


def callback(event_body):
    print(" [x] Received %r" % event_body)
    import time
    time.sleep(0.02)
    return dict(status=True)

from tq_event import tq_client

tq_client.connect(amqp_url,callback=callback)
tq_client.start_consume()