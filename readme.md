# rocksdb-profiler
This project aims to visualize the IO consumption and DB statistics.

## How to use

dependency:
1. [prometheus-cpp](https://github.com/jupp0r/prometheus-cpp)
2. docker

### install with cmake
```
mkdir build && cd build
cmake ..
sudo make install
```

### link with your project
simply use `find_package` in your `CMakeLists.txt`, then link with your executable.
```
find_package(rocksprofiler REQUIRED)
...
target_link_libraries(your_executable rocksprofiler)
```

### deploy the grafana dashboard
```
docker compose up
```

### Take a look at the dashboard
open your browser and go to `localhost:3000`, then login with `admin:admin`, you will see the dashboard.


## TODO
- [ ] heavily impact the performance, need to optimize