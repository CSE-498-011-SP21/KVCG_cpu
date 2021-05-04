CC = g++
CFLAGS = -Wall -Wextra -Werror -Wwrite-strings -pthread -std=c++17 -g
OPT = -O3

all: test_cache hopscotch hopscotch_v2

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

concurrent: common.o
	$(CC) $(CFLAGS) $(OPT) -c concurr_trad_hashmap.cpp

test_cache: seq_trad_hashmap.o concurr_trad_hashmap.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -o test_cache test_cache.cpp

linear: dummy.o test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -c linear.cpp

hopscotch: test_cache.cpp
	$(CC) $(CFLAGS) $(OPT) -c HopscotchWrapper.cpp

hopscotch_v2: HopscotchWrapper.o
	$(CC) $(CFLAGS) $(OPT) -o hopscotch_v2 -c hopscotch_v2.cpp

# Add executable file names here as I make them
clean:
	rm -f *.o linear
	rm -f *.o test_cache
	rm -f *.o hopscotch
	rm -f *.o hopscotch_v2

test: clean test_cache
	./test_cache -t 1 -v sequ