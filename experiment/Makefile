CC = g++
CFLAGS = -Wall -Wextra -Werror -Wwrite-strings -pthread -std=c++17 -g
OPT = -O3

all: test_cache

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

art: common.o
	$(CC) $(CFLAGS) $(OPT) -c adaptive_radix_trie.cpp

concurrent: common.o
	$(CC) $(CFLAGS) $(OPT) -c concurr_trad_hashmap.cpp

test_cache: seq_trad_hashmap.o concurr_trad_hashmap.o adaptive_radix_trie.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -o test_cache test_cache.cpp

# Add executable file names here as I make them
clean:
	rm -f *.o test_cache

test: clean test_cache
	./test_cache -t 1 -v sequ