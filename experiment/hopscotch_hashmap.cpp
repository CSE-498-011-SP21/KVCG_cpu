//Code is based on Hopscotch Hashing paper by Maurice Herlihy, Nir Shavit, and Moran Tzafrir
// Published in 2008

//Concurrent Hopscotch Hashmap Class

#include "common.h"
#include<vector>
#include<functional>
#include<iostream>
#include<math.h>
#include<mutex>
#include<shared_mutex>
#include<atomic>
#include<limits>

#define  _NULL_DELTA SHRT_MIN 

template <typename K, typename V>
class hopscotch_hashmap: public cpu_cache<K, V>{

    private:

        static const_short _null_delta = SHRT_MIN;

        struct Bucket{
            short volatile _first_delta;
            short volatile _next_delta;
            unsigned int volatile _hash;
            K volatile _key;
            V volatile _value;

            void init() {
                _first_delta	= _NULL_DELTA;
                _next_delta		= _NULL_DELTA;
                _hash			= _EMPTY_HASH;
                _key			= _EMPTY_KEY;
                _data			= _EMPTY_DATA;
            }
        };

        struct Segment{
            std::mutex _lock;
            _u_int32_t volatile _timestamp;
            
            void init(){
                _timestamp =0;
                _lock.init();
            }
        }

        //Fields
        _u_int32_t volatile _segment_shift;
        _u_int32_t volatile _segment_mask;
        _u_int32_t volatile _bucket_mask;
        const int _cache_mask;
        const bool _is_cacheline_alignment:;
        //const int _hop_range;

        Segment* volatile _segments;
        Bucket* volatile _table;

        static const _u_int32_t _INSERT_RANGE = 1024*4;
        static const _u_int32_t _RESIZE_FACTOR = 2;

        //Helper Methods
        Bucket* get_start_cacheline_bucket(Bucket* const bucket) {
		    return (bucket - ((bucket - _table) & _cache_mask)); //can optimize 
	    }

        K (*get_random_K)();
        V (*get_random_V)();


    public:
        // Constructor
        hopscotch_hashmap(int initial_capacity, int concurrency_level, int cache_line_size, bool is_optimize_cacheline, int hop_range, K (*get_random_K_arg)(), V (*get_random_V_arg)()){
            // num_buckets = init_buckets;
            // // Add check for if num_buckets is not a power of two
            // chain_length_max = init_chain_length;
            // hash0 = hash0_arg;
            i_bits = log2l(num_buckets);
            // get_random_K = get_random_K_arg;
            // get_random_V = get_random_V_arg;
            // resizing = false;
            // threads_working = 0;

            // // Allocate memory
            // table = new hash_entry*[num_buckets];
            // table_locks = new std::shared_timed_mutex[num_buckets];

            // for(int i=0; i>num_buckets; i++){
            //     table[i] = nullptr;
            // }
        }

        // Deconstructor
        ~hopscotch_hashmap(){
            // // printTables();
            // // I would lock, but this is the destructor so it's safe to say it will only be called when I don't need to worry about it
            // printStats();
            
            // for(int i=0; i<num_buckets; i++){

            //     hash_entry* current_node = table[i];

            //     if(current_node == nullptr){
            //         // This list is already empty
            //         continue;
            //     }

            //     hash_entry* next_node = current_node->next;

            //     while(next_node != nullptr){
            
            //         delete current_node;
            //         current_node = next_node;
            //         next_node = current_node->next;
            //     }

            //     delete current_node;
            // }
            // delete table;
            // delete table_locks;
        }

        //hash function
        int h(K key){
            return key % (int)pow(2, i_bits);
        }
        
        //bool pair returned represents <was add successful, did size increase>
        std::pair<bool, bool> add(K key_to_add, V val_to_add){
            
            const int hash = h(key_to_add);
            Segment& segment = getSegment(hash);
            //start bucket is the bucket that key to add maps to when hashed
            Bucket* const start_bucket(getBucket(segment, hash));

            //acquire lock on segement
            segment._lock.lock();

            if(contains(key)){
                segment._lock.unlock();
                //key already exists, return false
                return {false, false};
            }

            //use linear probing, starting at start_bucket to find an empty entry (free_bucket)
            Bucket* free_bucket = freeBucketInCacheline(segment, start_bucket);

            //first try to add to some free bucket in its orginal bucket's cache line
            if(null != free_bucket){
                addKeyBeginningList(start_bucket, free_bucket, hash, key_to_add, val_to_add);
                ++(segment_count);
                segment._lock.unlock();
                return {true, true};
            }

            //linearly prob to find an empty slot within the hop range of the key's orginal bucket
            free_bucket = nearestFreeBucket(segment, start_bucket);
            if (free_bucket != null){
                int free_dist = (free_bucket - start_bucket);
                if (free_dist > _hop_range || free_dist < -_hop_range){
                    //get the last bucket in the list of buckets originating in the key's original bucket
                    free_bucket = displaceFreeRange(segment, start_bucket, free_bucket);
                }
                //if it cant find the empty bucket above, apply sequence of hopscoth displacements
                //try to displace the free_bucket to reside within hop_range of original bucket of key
                if (null != free_bucket){
                    addKeyEndList(start_bucket, free_max_bucket, hash, key_to_add, val_to_add, last_bucket);
                    ++(segment._count);
                    segment._lock.unlock();
                    return {true, true};
                }
            }

            //sequence of displacements can't be performed, resize table and try again
            segment._lock.unlock();
            resize();
            return add(key_to_add, val_to_add);

        }

        bool remove(K to_remove){
            const int hash = h(to_remove);
            Segment& segment (getSegment(hash));
            Bucket* const start_bucket(getBucket(segment, hash));
            Bucket* last_bucket(null);
            Bucket* curr_bucket(start_bucket);
            segment._lock.acquire();
            short next_delta(curr_bucket -> _first_delta);

            do{
                if(_null_delta == next_delta){
                    segment._lock.release();
                    return false;
                }

                curr_bucket += next_delta;
                if(to_remove == curr_bucket->key){ //not sure if this is a proper way to check equality 
                    removeKey(segment, start_bucket, curr_bucket, last_bucket, hash);
                    if (_is_cacheline_alignment){ //optimize cache-line assignment
                        cacheLineAlignment(segment, curr_bucket);
                    }
                    segment._lock.release();
                    return true;
                } 
                last_bucket = curr_bucket;
                next_delta = curr_bucket->_next_delta;
            } while(true);
            return false;
        }

        bool contains(K to_find){
            const int hash = h(to_find);
            Segment& segment (getSegment(hash));
            Bucket* curr_bucket(getBucket(segment, hash));
            int start_timestamp;
            do{
                start_timestamp = segment._timestamp[hash & _timestamp_mask];
                short next_delta(curr_bucket->_first_delta);
                while(_null_delta != next_delta){
                    curr_bucket += next_delta;
                    if (key == curr_bucket->_key){ //Not sure if i can do this comparison like that
                        return true;
                    }
                    next_delta = curr_bucket->_next_delta;
                }
            }while(start_timestamp != segment._timestamp[hash & _timestamp_mask]);
            return false;
        }

        // I'm going to assume start < end
        std::vector<V> range_query(K start, K end){
            // std::cerr << "IN RANGE QUERY: " << start << ", " << end << std::endl;

            std::vector<V> values;

            if(start > end){
                return values;
            }

            // Hm... does range_query need to lock? Since it is traversing multiple buckets and they might change in-between leading to an inconsistent state?
            // My intuition is yes, unfortunately...

            // Block execution while resizing
            while(resizing){}
            threads_working += 1;

            // We do need to check TOCTOU here to make sure that we didn't start resizing because if we did then...
            // We have have had a situation where threads_working was 0 and the resize thread immediately started going
            // I could have checked resizing here, then the resize thread will go and set resizing to true and check threads_working and it's one
            // So we just need to make sure we haven't started resizing before threads_working was incremented
            if(resizing){
                // This thread needs to start over
                threads_working -= 1;
                return range_query(start, end);
            }

            unsigned long hashed_value_start = hash0(start);
            long index_start = hashed_value_start >> (32-i_bits);
            
            unsigned long hashed_value_end = hash0(end);
            long index_end = hashed_value_end >> (32-i_bits);

            for(long i=index_start; i<=index_end; i++){
                // std::cerr << "Locking " << i << std::endl;
                table_locks[i].lock_shared();
            }

            // Okay, so I just need to start at the first bucket and add all elements that are greater than start to my list
            hash_entry* current_node = table[index_start];

            // This is a special case, I am basically just searching a sorted linked list
            if(index_start == index_end){
                if(current_node == nullptr){
                    // Do nothing since there are no values to consider
                }else{
                    while(current_node->next != nullptr){
                        if(current_node->key >= start){
                            break;
                        }
                        current_node = current_node->next;
                    }

                    // Start adding values since current_node is now at the right place
                    while(current_node->next != nullptr){
                        if(current_node->in_use){
                            if(current_node->key <= end){
                                values.push_back(current_node->value);
                            }else{
                                // We don't have to look anymore
                                for(long i=index_start; i<=index_end; i++){
                                    // std::cerr << "Unlocking " << i << std::endl;
                                    table_locks[i].unlock_shared();
                                }

                                threads_working -= 1;
                                return values;
                            }
                        }
                        current_node = current_node->next;
                    }

                    if(current_node->in_use && current_node->key <= end){
                        values.push_back(current_node->value);
                    }
                }

                for(long i=index_start; i<=index_end; i++){
                    // std::cerr << "Unlocking " << i << std::endl;
                    table_locks[i].unlock_shared();
                }

                threads_working -= 1;
                return values;
            }

            if(current_node == nullptr){
                // There are no keys in this list so move to the next index
            }else{
                while(current_node->next != nullptr){
                    if(current_node->key >= start){
                        break;
                    }
                    current_node = current_node->next;
                }

                // Start adding values since current_node is now at the right place
                while(current_node->next != nullptr){
                    if(current_node->in_use){
                        values.push_back(current_node->value);
                    }
                    current_node = current_node->next;
                }

                if(current_node->in_use){
                    values.push_back(current_node->value);
                }
            }

            // Now we are going to go through all the lists until we reach the last one
            long index_current = index_start+1;
            while(index_current < index_end){
                current_node = table[index_current];

                // Check if the list is empty
                if(current_node == nullptr){
                    index_current++;                    
                    continue;
                }

                while(current_node->next != nullptr){
                    if(current_node->in_use){
                        values.push_back(current_node->value);
                    }
                    current_node = current_node->next;
                }
                
                if(current_node->in_use){
                    values.push_back(current_node->value);
                }

                index_current++;
            }

            // Finally, add all the elements in the last index that are less than or equal to the end key
            current_node = table[index_end];
            // Make sure the list exists before trying to traverse it
            if(current_node == nullptr){
                // Do nothing since this last list does not exist
            }else{
                while(current_node->next != nullptr && current_node->key <= end){
                    if(current_node->in_use){
                        values.push_back(current_node->value);
                    }
                    current_node = current_node->next;
                }

                // Check the last node, in case it was the last one in the list
                if(current_node->in_use && current_node->key <= end){
                    values.push_back(current_node->value);
                }
            }

            for(long i=index_start; i<=index_end; i++){
                // std::cerr << "Unlocking " << i << std::endl;
                table_locks[i].unlock_shared();
            }

            threads_working -= 1;
            return values;
        }

        long size(){
            // std::cerr << "IN SIZE" << std::endl;

            long total_elements = 0;
            
            for(int i=0; i<num_buckets; i++){
                hash_entry* current_node = table[i];

                if(current_node == nullptr){
                    // This list has nothing so...
                    continue;
                }

                while(current_node->next != nullptr){
                    // Check if the node is being used
                    if(current_node->in_use == true){
                        total_elements++;
                    }
                    current_node = current_node->next;
                }

                // Check last node
                if(current_node->in_use == true){
                    total_elements++;
                }
            }

            return total_elements;
        }

        void populate(long num_elements){
            // std::cerr << "IN POP" << std::endl;

            for(long i=0; i<num_elements; i++){
                // Ensures effective adds
                while(!add(get_random_K(), get_random_V()).second);
            }

            // printTables();
        }

        void printTables(){
            std::cout << "Table Size: " << num_buckets << std::endl;

            for(int i=0; i<num_buckets; i++){
                hash_entry* current_node = table[i];

                std::cout << "Table " << i << ": ";

                if(current_node == nullptr){
                    // This list has no elements so skip
                    std::cout << std::endl;
                    continue;
                }

                while(current_node->next != nullptr){
                    // Check if the node is being used
                    if(current_node->in_use == true){
                        std::cout << "(" << current_node->key << ", " << current_node->value << "), ";
                    }
                    current_node = current_node->next;
                }

                // Check last node
                if(current_node->in_use == true){
                    std::cout << "(" << current_node->key << ", " << current_node->value << ")";
                }

                std::cout << std::endl;
            }
        }

        void printStats(){
            std::cout << "Number of Buckets: " << num_buckets << std::endl;
            std::cout << "Number of Elements: " << size() << std::endl;

            long num_tables_empty = 0;

            for(int i=0; i<num_buckets; i++){
                hash_entry* current_node = table[i];

                if(current_node == nullptr){
                    // This list has no elements
                    num_tables_empty++;
                }
            }

            std::cout << "Number of Empty Buckets: " << num_tables_empty << std::endl;
            std::cout << "Percent of Buckets Not Used: " << num_tables_empty/(1.0 * num_buckets) << std::endl;
        }

};