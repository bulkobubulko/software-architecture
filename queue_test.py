import hazelcast
import threading
import time

client = hazelcast.HazelcastClient()

queue = client.get_queue("bounded-queue").blocking()

stop_event = threading.Event()

def producer():
    for i in range(100):
        queue.put(i)
        time.sleep(0.1)
    stop_event.set()

def consumer(id):
    while not stop_event.is_set():
        value = queue.take()
        print(f"Consumer {id} read: {value}")
        time.sleep(0.1)

producer_thread = threading.Thread(target=producer)
producer_thread.start()

consumer_thread1 = threading.Thread(target=consumer, args=(1,))
consumer_thread2 = threading.Thread(target=consumer, args=(2,))
consumer_thread1.start()
consumer_thread2.start()

producer_thread.join()
consumer_thread1.join()
consumer_thread2.join()

client.shutdown()
