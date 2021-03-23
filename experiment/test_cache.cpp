/**
 * This file will create versions of the cpu_caches based on parameters and do testing on them
 */
#include<iostream>
// #include "sequential.cpp"
#include<random>
#include<unistd.h>
#include<string.h>
#include<chrono>
#include<thread>
#include<atomic>

enum run_type{
    sequ = 1,
    concurr = 2
};

// Define a struct to hold arguments
struct my_args{
    int num_threads;
    double update_ratio;
    run_type version;
};

// Define a method for getting arguments (and validating them)
my_args get_args(int argc, char** argv){
    my_args params;
    params.num_threads = 1;
    // params.update_ratio = 50;
    params.version = sequ;

    int opt;
    // while((opt = getopt(argc, argv, "t:u:v:")) != -1){
    while((opt = getopt(argc, argv, "t:v:")) != -1){
        switch(opt){
            case 't':
                params.num_threads = atoi(optarg);
                break;
            case 'u':
                params.update_ratio = atoi(optarg);
                if(params.update_ratio < 0 || params.update_ratio > 100){
                    std::cout << "Please select between 0 and 100 (inclusive) for -u" << std::endl;
                    exit(1);
                }
                break;
            case 'v':
                if (!strcmp(optarg, "sequ"))
                {
                    params.version = sequ;
                }
                else if (!strcmp(optarg, "concurr"))
                {
                    params.version = concurr;
                }else{
                    std::cout << "Please use sequ, concurr, or transact for -v" << std::endl;
                    exit(1);
                }
                break;
        }
    }

    if(params.num_threads == 1 && params.version != sequ){
        std::cout << "Should use sequential version if we only have 1 thread." << std::endl;
        exit(1);
    }

    if(params.num_threads > 1 && params.version == sequ){
        std::cout << "Cannot use more than 1 thread for sequential version." << std::endl;
        exit(1);
    }

    return params;
}

long hash_int_0(int to_hash){
    return to_hash;
}

long hash_int_1(int to_hash){
    return to_hash+1;
}

int get_random_int(){
    return rand() % 10000000;
}

long hash_double_0(double to_hash){
    return (long)(to_hash * 10000);
}

long hash_double_1(double to_hash){
    return (long)(to_hash * 20000);
}

double get_random_double(){
    double my_rand = (double)rand()/(double)RAND_MAX; 
    return my_rand;
}

// This will be where I create the cpu_caches and call the appropriate methods
int main(int argc, char** argv){
    my_args params = get_args(argc, argv);

    std::cout << "Num Threads: " << params.num_threads << std::endl;
    std::cout << "Update Ratio: " << params.update_ratio << std::endl;
    std::cout << "Version (1=sequ, 2=concurr): " << params.version << std::endl;
    std::cout << std::endl;

    srand(100);

    cpu_cache<int> *my_test_set = NULL;

    // VARS for controlling program execution
    long INIT_CAP = 20000;
    long POP_SIZE = 20000;
    long NUM_OPS = 100000000;

    std::cout << "Initial Table Capacity: " << INIT_CAP << std::endl;
    std::cout << "Initial Size: " << POP_SIZE << std::endl;
    std::cout << "Number of Operations: " << NUM_OPS << std::endl;

    // Pick the data structure type that we are going to use
    switch(params.version){
        case sequ:
            // my_test_set = new sequential<int>(INIT_CAP, &hash_int_0, &hash_int_1, &get_random_int);
            break;
        case concurr:
            // my_test_set = new concurrent<int>(INIT_CAP, NUM_LOCKS, 8, 2, &hash_int_0, &hash_int_1, &get_random_int);
            break;
    }

    my_test_set->populate(POP_SIZE);

    std::atomic<int> successful_contains(0);
    std::atomic<int> failed_contains(0);
    std::atomic<int> successful_adds(0);
    std::atomic<int> failed_adds(0);
    std::atomic<int> successful_removes(0);
    std::atomic<int> failed_removes(0);

    // TO-DO: Right now, this function assumes int. How can I avoid that? Maybe call random function from my_test_set?
    auto do_work = [&](long NUM_OPS){
        // Make local variables for saving the number of effective v. ineffective ops
        long local_successful_contains = 0, local_failed_contains = 0, local_successful_adds = 0, local_failed_adds = 0, local_successful_removes = 0, local_failed_removes = 0;
        
        for(int i=0; i<NUM_OPS; i++){
            // std::cout << "On i: " << i << std::endl;
            int next_op = rand() % 100;
            int next_int = get_random_int();
            if(next_op < 50){
                // Do contains
                if(my_test_set->contains(next_int)){
                    local_successful_contains++;
                }else{
                    local_failed_contains++;
                }
            }else if(next_op < 75){
                // Do add
                if(my_test_set->add(next_int)){
                    local_successful_adds++;
                }else{
                    local_failed_adds++;
                }
            }else if(next_op < 100){
                // Do remove
                if(my_test_set->remove(next_int)){
                    local_successful_removes++;   
                }else{
                    local_failed_removes++;
                }
            }
        }

        // Add vars for counting operations to atomics
        successful_contains += local_successful_contains;
        failed_contains += local_failed_contains;
        successful_adds += local_successful_adds;
        failed_adds += local_failed_adds;
        successful_removes += local_successful_removes;
        failed_removes += local_failed_removes;
    };

    std::vector<std::thread> threads;
    // TO-DO: Should I be dividing the number of ops by the number of threads so that those with more threads don't have more work?
    long ops_per_thread = NUM_OPS / params.num_threads;


    auto start = std::chrono::high_resolution_clock::now();

    // Special case when num_threads == 1
    if(params.num_threads == 1){
        do_work(ops_per_thread);
    }else{
        for(int i=0; i<params.num_threads; i++){
            std::thread new_worker(do_work, ops_per_thread);
            threads.push_back(std::move(new_worker));
        }

        for(int i=0; i<params.num_threads; i++){
            threads.at(i).join();
        }
    }

    // End timer
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::microseconds time_elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    auto total_time = time_elapsed.count();

    // I just wanted a newline
    std::cout << std::endl;

    std::cout << "Total Time: " << total_time << "\n" << std::endl;

    std::cout << "Successful Contains: " << successful_contains << std::endl;
    std::cout << "Failed Contains: " << failed_contains << std::endl;
    std::cout << "Successful Adds: " << successful_adds << std::endl;
    std::cout << "Failed Adds: " << failed_adds << std::endl;
    std::cout << "Successful Removes: " << successful_removes << std::endl;
    std::cout << "Failed Removes: " << failed_removes << std::endl;
    std::cout << "Total Ops: " << successful_contains + failed_contains + successful_adds + failed_adds + successful_removes + failed_removes << std::endl;

    std::cout << std::endl;

    std::cout << "Expected Size: " << POP_SIZE + successful_adds - successful_removes << std::endl;
    std::cout << "Actual Size: " << my_test_set->size() << std::endl;

    // For debugging
    // my_test_set->printTables();

    delete my_test_set;
    return 0;
}

// In addition, I will need to save the data so I can make plots (or something)