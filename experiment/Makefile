CC = g++
CFLAGS = -Wall -Wextra -Werror -Wwrite-strings -pthread -std=c++17 -g
OPT = -O3

all: test_cache test_radix

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

radix: common.o
	$(CC) $(CFLAGS) $(OPT) -c radix_trie.cpp

concurrent: common.o
	$(CC) $(CFLAGS) $(OPT) -c concurr_trad_hashmap.cpp

test_cache: seq_trad_hashmap.o concurr_trad_hashmap.o radix_trie.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -o test_cache test_cache.cpp

test_radix: radix_trie.o test_radix.cpp
	$(CC) $(CFLAGS) $(OPT) -o test_radix test_radix.cpp

# Add executable file names here as I make them
clean:
	rm -f *.o test_cache

test: clean test_cache
	./test_cache -t 1 -v sequ