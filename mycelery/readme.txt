利用消息队列异步执行任务
python 3.6.5
tornado
rabbimq

两个队列：任务队列和任务完成结果队列

web端使用
tq_server
1.首次建立连接
def connect(amqp_url, task_queue=None, result_queue=None, event_container=None)
2.异步执行任务
async def do_async_task(body, timeout=None)


任务执行使用
tq_client
1.创建回调函数callback
2.建立连接
def connect( amqp_url, task_queue=None, result_queue=None, callback=None)
3.启动消费者
def start_consume()


比较粗糙，但也比较简单

测试执行顺序
consumer.py
tornado-server.py
tornado-req.py
