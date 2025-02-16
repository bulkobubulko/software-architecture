import hazelcast
import threading
import time

client = hazelcast.HazelcastClient()
distributed_map = client.get_map("distributed-map").blocking()

def reset_map():
    distributed_map.put("key", 0)

def increment_without_lock(client_id):
    start_time = time.time()
    
    for _ in range(10_000):
        value = distributed_map.get("key")
        value += 1
        distributed_map.put("key", value)

    elapsed_time = time.time() - start_time
    print(f"Client {client_id} (No Lock) - Time: {elapsed_time:.4f} sec")


def increment_with_pessimistic_lock(client_id):
    start_time = time.time()

    for _ in range(10_000):
        distributed_map.lock("key")
        try:
            value = distributed_map.get("key")
            value += 1
            distributed_map.put("key", value)
        finally:
            distributed_map.unlock("key")

    elapsed_time = time.time() - start_time
    print(f"Client {client_id} (Pessimistic Lock) - Time: {elapsed_time:.4f} sec")

def increment_with_optimistic_lock(client_id):
    start_time = time.time()

    for _ in range(10_000):
        while True:
            old_value = distributed_map.get("key")
            new_value = old_value + 1
            if distributed_map.replace_if_same("key", old_value, new_value):
                break

    elapsed_time = time.time() - start_time
    print(f"Client {client_id} (Optimistic Lock) - Time: {elapsed_time:.4f} sec")

def run_test(increment_function):
    reset_map()
    threads = []
    
    for i in range(3):
        thread = threading.Thread(target=increment_function, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    final_value = distributed_map.get("key")
    print(f"Final value: {final_value}\n")

print("Running test WITHOUT lock:")
run_test(increment_without_lock)

print("Running test WITH pessimistic lock:")
run_test(increment_with_pessimistic_lock)

print("Running test WITH optimistic lock:")
run_test(increment_with_optimistic_lock)

client.shutdown()
