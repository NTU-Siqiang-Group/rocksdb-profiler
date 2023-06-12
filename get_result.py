import requests as req
import matplotlib.pyplot as plt
import sys
import datetime
import time
import math

start = 0
end = 0

prometheus_url = 'http://127.0.0.1:9090/api/v1/query_range'
results = {'read_stat': [], 'write_stat': [], 'comp_end': [], 'flush_end': []}

if len(sys.argv) < 2:
  print('please input the offset (in second)')
  exit(0)

if len(sys.argv) == 2:
  offset = int(sys.argv[1])
  start = (datetime.datetime.now() - datetime.timedelta(seconds=offset)).timetuple()
  start = time.mktime(start)
  end = time.mktime(datetime.datetime.now().timetuple())
elif len(sys.argv) == 3:
  off1 = int(sys.argv[1])
  off2 = int(sys.argv[2])
  start = (datetime.datetime.now() - datetime.timedelta(seconds=off1)).timetuple()
  start = time.mktime(start)
  end = (datetime.datetime.now() - datetime.timedelta(seconds=off2)).timetuple()
  end = time.mktime(end)
else:
  print('too many arguments')
  exit(0)

params = {
  'start': start,
  'end': end,
  'step': '1s'
}

for key in results.keys():
  params['query'] = key
  r = req.get(prometheus_url, params=params)
  if r.status_code != 200:
    print('request failed')
    exit(0)
  data = r.json()['data']['result'][0]['values']
  for item in data:
    val = float(item[1])
    if key == 'comp_end' or key == 'flush_end':
      val *= 500
    if val > 0:
      val  = math.log(val)
    results[key].append(val)  

_, ax = plt.subplots(figsize=(70, 10))
ax.plot(results['read_stat'], label='read_latency')
ax.plot(results['write_stat'], label='write_latency')
ax.plot(results['comp_end'], label='compaction')
ax.plot(results['flush_end'], label='flush')
plt.savefig('result.png')