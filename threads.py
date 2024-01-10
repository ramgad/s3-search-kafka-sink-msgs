import threading
import time

def my_function():
    time.sleep(5)

# Create and start threads
threads = [threading.Thread(target=my_function) for _ in range(265)]
for thread in threads:
    thread.start()

# Monitor thread usage
while threading.active_count() > 1:  # Main thread is also counted
    print(f"Number of active threads: {threading.active_count() - 1}")
    time.sleep(1)

