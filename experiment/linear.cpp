#include "common.h"
#include <vector>
#include <functional>
#include <iostream>
#include <mutex>
#include <cmath>
#include <atomic>

//template for generic type
template <typename K, typename V>
class hash_node
{
    public:
        K key;
        V value;
        bool deleted;

        //Constructor of hashnode
        hash_node(K key, V value)
        {
            this->value = value;
            this->key = key;
            this->deleted = false;
        }
};

template <typename K, typename V>
class linear_hashmap: public cpu_cache<K,V>{
    private:
        struct bucket
        {
            std::vector<hash_node<K,V> > bucket_table; 
            std::mutex mtx;
        };

        // We use a vector since that is probably optimized for what I am going for 
        // (appending to the end and being able to index into any element quickly with underlying memory)
        std::vector<bucket*> map;

        //TODO: pick a good number for i?? Or do we start at 0 and let it incr itself
        // i is the number of bits to consider in the hash function (2^i, that is)
        int i = 0;
        //tracks number of buckets in the hashmap 
        std::atomic<u_int64_t> num_buckets{0};
        std::atomic<u_int64_t> num_records{0};
        // p is the value we use to know when we should add a new bucket to the hash map
        // That is, if it is exceded by r/n, then we add a new bucket
        int p = 1;
        
        // Declare function pointers
        K (*get_random_K)();
        V (*get_random_V)();

        //global lock
        //std::mutex map_lock;


    public:

        // Constructor
        linear_hashmap(K (*get_random_K_arg)(), V (*get_random_V_arg)()){
            
            get_random_K = get_random_K_arg;
            get_random_V = get_random_V_arg;

            // This reserve is to prevent adding new buckets from being expensive until we increment i
            long init_buckets = pow(2,i);
            this->map.reserve(init_buckets);

            // Make my first bucket so that there is 1 to start with always
            bucket* b = new bucket();
            this->map.push_back(b);
            ++num_buckets;

        }

        // Deconstructor
        ~linear_hashmap(){
            //do nothing for now
        }

        u_int64_t hash(K key){
            return key % (int)pow(2, i);
        }
        
        //bool pair returned represents <was add successful, did size increase>
        std::pair<bool, bool> add(K key_to_add, V val_to_add){

            // Apply hash function to find index for given key
            u_int64_t hash_index = hash(key_to_add);

            if(hash_index >= num_buckets){
                // This should only ever happen ONCE, if at all
                // That is because there will always be at least 2^(i-1) buckets SO by just ignoring the leading 1, the other bucket should exist
                hash_index /= 2;
            }

            //Lock the bucket
            this->map.at(hash_index)->mtx.lock();

            // Check if the key is already in the data structure
            bucket* search_bucket = this->map.at(hash_index);
            for(unsigned int index=0; index<search_bucket->bucket_table.size(); index++){
                if(key_to_add == search_bucket->bucket_table.at(index).key){
                    //Update the value if key already exists
                    search_bucket->bucket_table.at(index).value = val_to_add;
                    this->map.at(hash_index)->mtx.unlock();
                    return {true, false};
                }
            }

            //add the entry
            this->map.at(hash_index)->bucket_table.push_back(hash_node<K,V>(key_to_add, val_to_add));
            num_records++;

            //std::atomic_thread_fence(std::memory_order_acquire);
            //check if p is violated and make necessary adjustments
            double p_check = (double)num_records/num_buckets;
            std::cerr << "P check: " << p_check << std::endl;
            if(p < p_check){
                // p is violated!
                std::cerr << "p was violated, fixing..." << std::endl;

                if(num_buckets == pow(2, i)){
                    // We're at the max number of buckets! We need to remake the whole hashmap
                    //TODO: CREATE FENCE here so that we don't race to incr num_buckets
                    std::atomic_thread_fence(std::memory_order_acquire);

                    //unlock so that we can lock every single bucket
                    this->map.at(hash_index)->mtx.unlock();
                    //lock every bucket first
                    for(size_t i = 0; i < this->map.size(); i++){
                        this->map.at(i)->mtx.lock();
                    }

                    //const std::lock_guard<std::mutex> lock(map_lock);

                    

                    // New hash map we are going to use
                    std::vector<bucket*> new_hash_map = std::vector<bucket*>();
                    
                    // This reserve is to prevent adding new buckets from being expensive until we increment i
                    new_hash_map.reserve(pow(2, i));

                    // Make new hash map with new buckets
                    for(uint64_t j=0; j<num_buckets+1; j++){
                        bucket* b = new bucket();
                        new_hash_map.push_back(b);
                    }
                    
                    

                    // Rehash old values into new values
                    for(auto it=this->map.begin(); it<this->map.end(); it++){
                        bucket* current_bucket = *it;
                        for(unsigned int index=0; index<current_bucket->bucket_table.size(); index++){
                            uint64_t hash_index = hash(current_bucket->bucket_table.at(index).key);
                            if(hash_index >= num_buckets){
                                // This should only ever happen ONCE per element, if at all
                                // That is because there will always be at least 2^(i-1) buckets SO by just ignoring the leading 1, the other bucket should exist
                                hash_index /= 2;
                            }

                            if(hash_index >= num_buckets){
                                // For debugging, just check that above assertion is always true
                                std::cerr << "Uh-oh! The hash index is still greater than the number of buckets!" << std::endl;
                            }

                            // We know there are no dups so we don't have to check for that
                            new_hash_map.at(hash_index)->bucket_table.push_back(hash_node<K, V>(current_bucket->bucket_table.at(index).key, current_bucket->bucket_table.at(index).value));
                            // Also, num_records has already been incremented so don't worry about that either
                        }
                    }

                    i++;
                    ++num_buckets;

                    //unlock every bucket 
                    for(size_t i = 0; i < this->map.size(); i++){
                        this->map.at(i)->mtx.unlock();
                    }

                    // Assign the hash_map to the new one that was just made
                    this->map = new_hash_map;
                    return {true, true};
                }else{
                    // I can just add a new bucket and remap the other bucket that was being used for all the values in this bucket
                    this->map.push_back(new bucket());
                    ++num_buckets;
                    
                    // Now, go to other bucket holding some of this bucket's values and add them here, if appropriate
                    bucket* old_bucket = this->map[(num_buckets-1)/2];
                    std::vector<int> indicies_to_remove_from_old_bucket;
                    // Iterate over bucket and remove entries that should be added to the new bucket
                    for(unsigned int index=0; index<old_bucket->bucket_table.size(); index++){
                        if(hash(old_bucket->bucket_table.at(index).key) == (num_buckets-1)){
                            // This value entry should be in the new bucket
                            this->map.at(num_buckets-1)->bucket_table.push_back(old_bucket->bucket_table.at(index));
                        }else{
                            // This value entry should be in the old bucket
                            indicies_to_remove_from_old_bucket.push_back(index);
                        }
                    }

                    // Clean up old bucket
                    for(unsigned int index=0; index<indicies_to_remove_from_old_bucket.size(); index++){
                        // Must use the hash_map since old_bucket is a deep copy!
                        // This is super gross, FIX THIS to be more efficient! Just use an iterator on the above loop, comon MAtt
                        this->map.at((num_buckets-1)/2)->bucket_table.erase(this->map.at((num_buckets-1)/2)->bucket_table.begin() + indicies_to_remove_from_old_bucket[index]);
                    }
                }
            }   
            this->map.at(hash_index)->mtx.unlock();
            return {true, true};
        }

        bool remove(K to_remove){
            // Apply hash function to find index for given key
            u_int64_t hash_index = hash(to_remove);

            if(hash_index >= num_buckets){
                // This should only ever happen ONCE, if at all
                // That is because there will always be at least 2^(i-1) buckets SO by just ignoring the leading 1, the other bucket should exist
                hash_index /= 2;
            }

            //Lock the bucket
            const std::lock_guard<std::mutex> lock(this->map.at(hash_index)->mtx);
            // Check if the key is already in the data structure
            bucket* search_bucket = this->map.at(hash_index);
            for(unsigned int index=0; index<search_bucket->bucket_table.size(); index++){
                if(to_remove == search_bucket->bucket_table.at(index).key){
                    //Mark as deleted if key exists
                    search_bucket->bucket_table.at(index).deleted = true;
                    return true;
                }
            }
            return false;
        }

        bool contains(K to_find){
            // Apply hash function to find index for given key
            u_int64_t hash_index = hash(to_find);

            if(hash_index >= num_buckets){
                // This should only ever happen ONCE, if at all
                // That is because there will always be at least 2^(i-1) buckets SO by just ignoring the leading 1, the other bucket should exist
                hash_index /= 2;
            }

            //Lock the bucket
            const std::lock_guard<std::mutex> lock(this->map.at(hash_index)->mtx);
            // Check if the key is already in the data structure
            bucket* search_bucket = this->map.at(hash_index);
            for(unsigned int index=0; index<search_bucket->bucket_table.size(); index++){
                if(to_find == search_bucket->bucket_table.at(index).key){
                    return true;
                }
            }
            return false;
        }

        std::vector<V> range_query(K start, K end){
            std::vector<V> result;

            // Apply hash function to find index for start key
            u_int64_t start_index = hash(start);
            if(start_index >= num_buckets){
                // This should only ever happen ONCE, if at all
                // That is because there will always be at least 2^(i-1) buckets SO by just ignoring the leading 1, the other bucket should exist
                start_index /= 2;
            }
            //Query for 1 key
            if(start == end){
                //Lock the bucket
                const std::lock_guard<std::mutex> lock(this->map.at(start_index)->mtx);
                // Check if the key is already in the data structure
                bucket* search_bucket = this->map.at(start_index);
                for(unsigned int index=0; index<search_bucket->bucket_table.size(); index++){
                    if(start == search_bucket->bucket_table.at(index).key){
                        //return value if found
                        result.push_back(search_bucket->bucket_table.at(index).value);
                        return result;
                    }
                }
            }
            //Query is for more than 1 key
            u_int64_t end_index = hash(end);
            bucket* start_bucket = this->map.at(start_index);

            if (start_index == end_index){
                //lock bucket
                const std::lock_guard<std::mutex> lock(start_bucket->mtx);
                //just search through the 1 bucket until you get to the end key
                for(auto it=start_bucket->bucket_table.begin(); it<start_bucket->bucket_table.end(); it++){
                    hash_node<K,V> curr_node = *it;
                    if (curr_node.key <= end){
                        result.push_back(curr_node.value);
                    }
                }
                return result;
            }

            //Range query for more than 1 bucket
            for(uint64_t i = start_index; i <= end_index; i++){
                bucket* search_bucket = this->map.at(i);
                for(uint64_t j = 0; j < search_bucket->bucket_table.size(); j++){
                    if(search_bucket->bucket_table.at(j).key <= end){
                        result.push_back(search_bucket->bucket_table.at(j).value);
                    }
                }
            }
            return result;
        }

        long size(){
            long total_elements = 0;
            
            for(uint64_t i=0; i<this->num_buckets; i++){
                bucket* search_bucket = this->map.at(i);
                for (uint64_t j=0; j<search_bucket->bucket_table.size(); j++){
                    if(search_bucket->bucket_table.at(j).deleted == false){
                        total_elements++;
                    }
                }
            }

            return total_elements;
        }

        void populate(long num_elements){
            for(long i=0; i<num_elements; i++){
                add(get_random_K(), get_random_V());
            }
        }

        void printMap(){
            int counter = 0;
            for(auto it=this->map.begin(); it<this->map.end(); it++){
                bucket* current_bucket = *it;
                std::cout << "Bucket " << counter << ": ";
                for(unsigned int index=0; index<current_bucket->bucket_table.size(); index++){
                    std::cout << current_bucket[index].key << ", " << current_bucket[index].value << " -> ";
                }
                std::cout << std::endl;
                counter++;
            }
        }
};