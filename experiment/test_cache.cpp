/**
 * This file will create versions of the cpu_caches based on parameters and do testing on them
 */
#include<iostream>
#include "seq_trad_hashmap.cpp"
#include "linear.cpp"
#include "concurr_trad_hashmap.cpp"
#include "hopscotch_v3.cpp"
#include<random>
#include<unistd.h>
#include<string.h>
#include<chrono>
#include<thread>
#include<atomic>

enum run_type{
    sequ = 1,
    concurr = 2,
    linear = 3,
    hopscotch = 4
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
    params.update_ratio = 50;
    params.version = hopscotch;

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

    // if(params.num_threads == 1 && params.version != sequ){
    //     std::cout << "Should use sequential version if we only have 1 thread." << std::endl;
    //     exit(1);
    // }

    // if(params.num_threads > 1 && params.version == sequ){
    //     std::cout << "Cannot use more than 1 thread for sequential version." << std::endl;
    //     exit(1);
    // }

    return params;
}

long hash_int_0(int to_hash){
    return to_hash;
}

// Globals are bad... but I don't care
std::default_random_engine generator(42);
std::uniform_int_distribution<int> distribution(0, 2147483647);
// std::uniform_int_distribution<int> distribution(0, 100000000);
// std::uniform_int_distribution<int> distribution(0, 100);

int get_random_int(){
    return distribution(generator);
}

// This will be where I create the cpu_caches and call the appropriate methods
int main(int argc, char** argv){
    my_args params = get_args(argc, argv);

    std::cout << "Num Threads: " << params.num_threads << std::endl;
    std::cout << "Update Ratio: " << params.update_ratio << std::endl;
    std::cout << "Version (1=sequ, 2=concurr): " << params.version << std::endl;
    std::cout << std::endl;

    std::default_random_engine option_generator(42);
    std::uniform_int_distribution<int> option_distribution(0, 99);

    cpu_cache<int, int> *my_test_set = NULL;

    // VARS for controlling program execution
    long INIT_CAP = 4096*32*16;
    long POP_SIZE = 2048*32*16;
    long NUM_OPS = 100000000;

    // long INIT_CAP = 8;
    // long POP_SIZE = 0;
    // long NUM_OPS = 10000000;
    
    std::cout << "Initial Table Capacity: " << INIT_CAP << std::endl;
    std::cout << "Initial Size: " << POP_SIZE << std::endl;
    std::cout << "Number of Operations: " << NUM_OPS << std::endl;

    // Pick the data structure type that we are going to use
    switch(params.version){
        case sequ:
            my_test_set = new seq_traditional_hash_map<int, int>(INIT_CAP, 16, &hash_int_0, &get_random_int, &get_random_int);
            // my_test_set = new seq_traditional_hash_map<int, int>(INIT_CAP, 10000, &hash_int_0, &get_random_int, &get_random_int);
            break;
        case concurr:
            my_test_set = new concurr_traditional_hash_map<int, int>(INIT_CAP, 16, &hash_int_0, &get_random_int, &get_random_int);
            break;
        case linear:
            my_test_set = new linear_hashmap<int, int>(&get_random_int, &get_random_int);
            break;
        case hopscotch:
            //my_test_set = new hopscotch_v3<int, int>(32*1024, 16, 64, true, &get_random_int, &get_random_int, &hash_int_0);  
            my_test_set = new hopscotch_v3<int, int>(INIT_CAP, params.num_threads, 64, true, &get_random_int, &get_random_int, &hash_int_0);  
    }

    my_test_set->populate(POP_SIZE);

    std::atomic<int> successful_contains(0);
    std::atomic<int> failed_contains(0);
    std::atomic<int> successful_range_queries(0);
    std::atomic<int> failed_range_queries(0);
    std::atomic<int> successful_adds(0);
    std::atomic<int> failed_adds(0);
    std::atomic<int> successful_updates(0);
    // As of right now, failed_updates is not used, but that might be ok
    std::atomic<int> failed_updates(0);
    std::atomic<int> successful_removes(0);
    std::atomic<int> failed_removes(0);

    // TO-DO: Right now, this function assumes int. How can I avoid that? Maybe call random function from my_test_set?
    auto do_work = [&](long number_of_ops){
        // Make local variables for saving the number of effective v. ineffective ops
        long local_successful_contains = 0, local_failed_contains = 0, local_successful_range_queries = 0, local_failed_range_queries = 0, local_successful_adds = 0, local_failed_adds = 0, local_successful_updates = 0, local_failed_updates = 0, local_successful_removes = 0, local_failed_removes = 0;

        for(int i=0; i<number_of_ops; i++){
            // std::cout << "On i: " << i << std::endl;

            // I should do all the random number generation everytime so runs are consistent even if I adjust parameters

            int next_op = option_distribution(option_generator);
            int number_of_elements = option_distribution(option_generator);
            int next_key = get_random_int();
            int next_val = get_random_int();
            if(next_op < 10){
                // Do range query
                std::vector<int> returned_values = my_test_set->range_query(next_key, (next_key+number_of_elements));

                // std::cout << "Start: " << next_key << ", End: " << next_key+number_of_elements << std::endl;
                // for(unsigned int i=0; i < returned_values.size(); i++)
                //     std::cout << returned_values.at(i) << ' ';
                // std::cout << std::endl;

                if(!returned_values.empty()){
                    local_successful_range_queries++;
                }else{
                    local_failed_range_queries++;
                }
            }else if(next_op < 50){
                // Do contains
                if(my_test_set->contains(next_key)){
                    local_successful_contains++;
                }else{
                    local_failed_contains++;
                }
            }else if(next_op < 75){
                // Do add/update
                std::pair<bool, bool> result = my_test_set->add(next_key, next_val);
                if(result.second){
                    local_successful_adds++;
                }else if(result.first){
                    local_successful_updates++;
                }else{
                    local_failed_adds++;
                }
            }else if(next_op < 100){
                // Do remove
                if(my_test_set->remove(next_key)){
                    local_successful_removes++;   
                }else{
                    local_failed_removes++;
                }
            }
        }

        // Add vars for counting operations to atomics
        successful_contains += local_successful_contains;
        failed_contains += local_failed_contains;
        successful_range_queries += local_successful_range_queries;
        failed_range_queries += local_failed_range_queries;
        successful_adds += local_successful_adds;
        failed_adds += local_failed_adds;
        successful_updates += local_successful_updates;
        failed_updates += local_failed_updates;
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
    std::cout << "Successful Range Queries: " << successful_range_queries << std::endl;
    std::cout << "Failed Range Queries: " << failed_range_queries << std::endl;
    std::cout << "Successful Adds: " << successful_adds << std::endl;
    std::cout << "Failed Adds: " << failed_adds << std::endl;
    std::cout << "Successful Updates: " << successful_updates << std::endl;
    std::cout << "Failed Updates: " << failed_updates << std::endl;
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