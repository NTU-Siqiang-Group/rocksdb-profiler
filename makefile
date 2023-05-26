all:
	rm -f test.txt
	g++ -std=c++17 -pthread -o main.out main.cc monitor/io_monitor.cc
	./main.out