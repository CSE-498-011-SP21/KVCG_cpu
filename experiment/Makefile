CC = g++
CFLAGS = -Wall -Wextra -Werror -Wwrite-strings -pthread -std=c++14
OPT = -O3 -g

all: test_cache linear

common:
	$(CC) $(CFLAGS) $(OPT) -c common.h

# Need to add pre-reqs when I make the other files
# sequential: common.o
# 	$(CC) $(CFLAGS) $(OPT) -c sequential.cpp

# concurrent: common.o
# 	$(CC) $(CFLAGS) $(OPT) -c concurrent.cpp

# transactional: common.o
# 	$(CC) $(CFLAGS) $(OPT) -c transactional.cpp

traditional: common.o
	$(CC) $(CFLAGS) $(OPT) -c seq_trad_hashmap.cpp

test_cache: seq_trad_hashmap.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -o test_cache test_cache.cpp

linear: dummy.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -c linear.cpp

# Add executable file names here as I make them
clean:
	rm -f *.o linear
	rm -f *.o test_cache

test: clean test_cache
	./test_cache -t 1 -v sequ