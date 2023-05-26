import socket
import time
import json
import multiprocessing as mp

# data format:
# (stats_type, (stats_data, ...))
# stats_type: "compaction_start", stats_data: (start_time, job_id, start_level, output_level)
# stats_type: "compaction_end", stats_data: (end_time, job_id)
# stats_type: "io_stats": stats_data: (time, read_speed, write_speed)
# stats_type: "flush_start", stats_data: (start_time, job_id)
# stats_type: "flush_end", stats_data: (end_time, job_id)
# stats_type: "get", stats_data: (time, latency)
# stats_type: "put", stats_data: (time, latency)

def start_server(port=8080, queue=None):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind(('', port))
    s.listen(1) # only allow one connection
    conn, _ = s.accept()
    with conn:
      while True:
        data = conn.recv(1024)
        queue.put(data)

def start_printing(queue=None):
  while True:
    if queue.empty():
      time.sleep(1)
      continue
    data = queue.get()
    try:
      data = json.loads(data)
    except:
      print("invalid json data")
      continue
    print(data)

if __name__ == "__main__":
  queue = mp.Queue()
  server = mp.Process(target=start_server, args=(8080, queue))
  printer = mp.Process(target=start_printing, args=(queue,))
  server.start()
  printer.start()
  server.join()
  printer.join()