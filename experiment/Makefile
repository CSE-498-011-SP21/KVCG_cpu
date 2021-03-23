CC = g++
CFLAGS = -Wall -Wextra -Werror -Wwrite-strings
OPT = -O3

all: test_cache

common:
	$(CC) $(CFLAGS) $(OPT) -c common.h

# Need to add pre-reqs when I make the other files
sequential: common.o
	$(CC) $(CFLAGS) $(OPT) -c sequential.cpp

concurrent: common.o
	$(CC) $(CFLAGS) $(OPT) -c concurrent.cpp

transactional: common.o
	$(CC) $(CFLAGS) $(OPT) -c transactional.cpp

test_cache: sequential.o concurrent.o transactional.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -o test_cache test_cache.cpp

# Add executable file names here as I make them
clean:
	rm -f *.o test_cache

test: clean test_cache
	./test_cache -t 1 -v sequ