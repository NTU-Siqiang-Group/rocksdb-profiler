all:
	rm -f test.txt
	g++ -std=c++17 -pthread -lprometheus-cpp-pull -lprometheus-cpp-core -lz \
		 -o main.out main.cc monitor/io_monitor.cc
	./main.out