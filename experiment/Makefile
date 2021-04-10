CC = g++
CFLAGS = -Wall -Wextra -Werror -Wwrite-strings -pthread -g
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

test_cache: adaptive_radix_trie.o seq_trad_hashmap.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -o test_cache test_cache.cpp

# Add executable file names here as I make them
clean:
	rm -f *.o test_cache

test: clean test_cache
	./test_cache -t 1 -v sequ