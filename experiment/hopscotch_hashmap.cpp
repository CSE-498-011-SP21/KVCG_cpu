#include "common.h"
#include<vector>
#include<functional>
#include<iostream>
#include<math.h>
#include<mutex>
#include<shared_mutex>
#include<atomic>


template <typename K, typename V>
class hopscotch_hashmap: public cpu_cache<K, V>{

    struct hash_entry{
        K key;
        V value;
        bool in_use;
        hash_entry* next;

        hash_entry(){
            key=0;
            value=0;
            in_use=false;
            next=nullptr;
        }
    };

    private:
        long num_buckets;
        int chain_length_max;
        int i_bits;
        hash_entry** table;
        std::shared_timed_mutex* table_locks;

        std::atomic<bool> resizing;
        std::atomic<int> threads_working;

        // Declare function pointers
        long (*hash0)(K);
        K (*get_random_K)();
        V (*get_random_V)();

        std::pair<bool, bool> lock_free_add(K key_to_add, V val_to_add){
            // std::cerr << "IN LOCK FREE ADD: " << key_to_add << ", " << val_to_add << std::endl;
            // This is a special method that will always receive nodes in order to add

            unsigned long hashed_value = hash0(key_to_add);
            long index = hashed_value >> (32-i_bits);

            hash_entry* current_node = table[index];

            hash_entry* new_node = new hash_entry;
            new_node->key = key_to_add;
            new_node->value = val_to_add;
            new_node->in_use = true;
            // This is done by the constructor but I'll leave it for good measure
            new_node->next = nullptr;

            // List is empty, we need to start the list
            if(current_node == nullptr){
                table[index] = new_node;

                // std::cerr << "This is happening, right?" << std::endl;

                return std::make_pair(true, true);
            }

            while(current_node->next != nullptr){
                // std::cerr << "here?" << std::endl;
                current_node = current_node->next;
            }

            current_node->next = new_node;
            return std::make_pair(true, true);
        }

        void resize(){
            // std::cerr << "IN RESIZE, New Num Buckets: " << (num_buckets*2) << std::endl;
            // long original_size = size();
            // Do I need this to be strong?
            bool mine(false);
            if(!resizing.compare_exchange_strong(mine, true)){
                // This failed so we must already be resizing
                return;
            }else{
                // Do nothing
            }

            // Block to wait for all threads to finish whatever they're doing
            // Specifically, all threads should be at the top of their next method, waiting
            while(threads_working > 1){}

            hash_entry** old_table = table;
            std::shared_timed_mutex* old_table_locks = table_locks;
            long num_buckets_old = num_buckets;
            std::shared_timed_mutex* new_table_locks = new std::shared_timed_mutex[num_buckets*2];

            // Lock on all the buckets
            // Could I use a shared lock for this? Since I just want to make sure the values won't change, not that they aren't being read. Interesting
            // for(int i=0; i<num_buckets_old; i++){
            //     old_table_locks[i].lock();
            // }

            // Do I just need to init to nullptr? LEFT OFF HERE!

            // Must adjust these AFTER locking
            table = new hash_entry*[num_buckets*2];
            num_buckets *= 2;
            i_bits += 1;

            for(int i=0; i<num_buckets; i++){
                table[i] = nullptr;
            }

            // Iterate over all buckets
            for(int i=0; i<num_buckets_old; i++){
                hash_entry* current_node = old_table[i];

                if(current_node == nullptr){
                    // This list is already empty
                    continue;
                }

                while(current_node->next != nullptr){
                    // Check if the node is being used
                    if(current_node->in_use == true){
                        lock_free_add(current_node->key, current_node->value);
                    }
                    hash_entry* next_node = current_node->next;

                    delete current_node;
                    current_node = next_node;
                }

                // Check last node
                if(current_node->in_use == true){
                    lock_free_add(current_node->key, current_node->value);
                }

                delete current_node;
            }

            // if(original_size != size()){
            //     std::cout << "ORIGINAL SIZE: " << original_size << std::endl;
            //     std::cout << "WRONG" << std::endl;
            //     std::cout << "New size: " << size() << std::endl;
            // }

            table_locks = new_table_locks;

            // THIS will make everything go again
            resizing=false;

            // Unlock on all the buckets (is this necessary?)
            // for(int i=0; i<num_buckets_old; i++){
            //     old_table_locks[i].unlock();
            // }

            // Delete all remaining pointers
            delete old_table;
            delete old_table_locks;
        }

    public:
        // Constructor
        hopscotch_hashmap(long init_buckets, int init_chain_length, long (*hash0_arg)(K), K (*get_random_K_arg)(), V (*get_random_V_arg)()){
            num_buckets = init_buckets;
            // Add check for if num_buckets is not a power of two
            chain_length_max = init_chain_length;
            hash0 = hash0_arg;
            i_bits = log2l(num_buckets);
            get_random_K = get_random_K_arg;
            get_random_V = get_random_V_arg;
            resizing = false;
            threads_working = 0;

            // Allocate memory
            table = new hash_entry*[num_buckets];
            table_locks = new std::shared_timed_mutex[num_buckets];

            for(int i=0; i>num_buckets; i++){
                table[i] = nullptr;
            }
        }

        // Default init_add_limit to 1
        hopscotch_hashmap(long init_buckets, long (*hash0_arg)(K), K (*get_random_K_arg)(), V (*get_random_V_arg)()) : hopscotch_hashmap(init_buckets, 1, hash0_arg, get_random_K_arg, get_random_V_arg){}

        // Deconstructor
        ~hopscotch_hashmap(){
            // printTables();
            // I would lock, but this is the destructor so it's safe to say it will only be called when I don't need to worry about it
            printStats();
            
            for(int i=0; i<num_buckets; i++){

                hash_entry* current_node = table[i];

                if(current_node == nullptr){
                    // This list is already empty
                    continue;
                }

                hash_entry* next_node = current_node->next;

                while(next_node != nullptr){
            
                    delete current_node;
                    current_node = next_node;
                    next_node = current_node->next;
                }

                delete current_node;
            }
            delete table;
            delete table_locks;
        }
        
        std::pair<bool, bool> add(K key_to_add, V val_to_add){
            // std::cerr << "IN ADD: " << key_to_add << ", " << val_to_add << std::endl;

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
                return add(key_to_add, val_to_add);
            }

            unsigned long hashed_value = hash0(key_to_add);
            long index = hashed_value >> (32-i_bits);

            // Keep track of the number of nodes to make sure that we don't have too many
            int linked_list_counter = 0;

            table_locks[index].lock();

            hash_entry* previous_node = table[index];
            hash_entry* current_node = table[index];
            bool first_node = false;

            // List is empty, we need to start the list
            if(current_node == nullptr){
                hash_entry* new_node = new hash_entry;
                new_node->key = key_to_add;
                new_node->value = val_to_add;
                new_node->in_use = true;
                // This is done by the constructor but I'll leave it for good measure
                new_node->next = nullptr;
                table[index] = new_node;

                table_locks[index].unlock();

                threads_working -= 1;
                return std::make_pair(true, true);
            }

            // If key is less than the first key in the data structure, we are adding it first
            if(key_to_add < current_node->key){
                first_node = true;
            }

            while(current_node->next != nullptr && current_node->key < key_to_add){
                previous_node = current_node;
                current_node = current_node->next;
                linked_list_counter++;
            }

            // If we have broken out of the loop, one of three things has happened.
            // 1. We are at the last node
            // 2. We have found a node that has the same key value as our insert
            // 3. We are at the position to add the new node in

            // First, we just check if the nodes have the same key and handle it
            if(key_to_add == current_node->key){
                current_node->value = val_to_add;
                if(current_node->in_use == true){
                    table_locks[index].unlock();
                    threads_working -= 1;
                    return std::make_pair(true, false);
                }
                current_node->in_use = true;
                table_locks[index].unlock();
                threads_working -= 1;
                return std::make_pair(true, true);
            }

            // Then, check if we are at the last node, and if we are, if our new key is > it or not
            bool last_node = false;
            if(current_node->next == nullptr){
                // We are at the last node so we should check if we should add our new node before or after this node
                if(key_to_add > current_node->key){
                    // We should add this node to the last spot in the list
                    last_node = true;
                }
            }

            // If we get here, we must add the new node into the linked list after previous_node
            // Before I can add it though, I must make sure that the linked list will not become too long
            while(current_node->next != nullptr){
                // std::cerr << "hung?2" << std::endl;
                current_node = current_node->next;
                linked_list_counter++;
            }

            // Add one more for the node we just ended on
            linked_list_counter++;

            // If we make it here, then the key is not already in the linked list so we need to add it
            // Before that, let's make sure the linked list won't be too long when I add another node
            if(linked_list_counter > chain_length_max){
                // We need to resize
                table_locks[index].unlock();
                resize();
                threads_working -= 1;
                return add(key_to_add, val_to_add);
            }

            // If we make it here, then we add the new item to the linked list AFTER previous_node
            hash_entry* new_node = new hash_entry;
            new_node->key = key_to_add;
            new_node->value = val_to_add;
            new_node->in_use = true;

            // NOTE: Important to change the pointer that's available already last since reads are allowed without locks

            // If we are still on the first node, then we need to use the pointer from our table
            if(first_node){
                new_node->next = table[index];
                table[index] = new_node;
            }else if(last_node){
                new_node->next = nullptr;
                current_node->next = new_node;
            }else{
                new_node->next = previous_node->next;
                previous_node->next = new_node;
            }

            table_locks[index].unlock();

            threads_working -= 1;
            return std::make_pair(true, true);
        }

        bool remove(K to_remove){
            // std::cerr << "IN REMOVE: " << to_remove << std::endl;

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
                return remove(to_remove);
            }

            unsigned long hashed_value = hash0(to_remove);
            long index = hashed_value >> (32-i_bits);

            table_locks[index].lock();

            hash_entry* current_node = table[index];

            if(current_node == nullptr){
                // Well there are no nodes so forget it
                table_locks[index].unlock();
                threads_working -= 1;
                return false;
            }

            while(current_node->next != nullptr && current_node->key < to_remove){
                current_node = current_node->next;
            }

            // If we make it here then we just need to check the node and then return
            if(to_remove == current_node->key){
                if(current_node->in_use == false){
                    // Key is already removed
                    table_locks[index].unlock();
                    threads_working -= 1;
                    return false;
                }else{
                    current_node->in_use = false;
                    table_locks[index].unlock();
                    threads_working -= 1;
                    return true;
                }                
            }

            table_locks[index].unlock();

            // Key not in data structure so return
            threads_working -= 1;
            return false;
        }

        bool contains(K to_find){
            // std::cerr << "IN CONTAINS: " << to_find << std::endl;

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
                return contains(to_find);
            }

            unsigned long hashed_value = hash0(to_find);
            long index = hashed_value >> (32-i_bits);

            hash_entry* current_node = table[index];

            if(current_node == nullptr){
                // Well there are no nodes so forget it
                threads_working -= 1;
                return false;
            }

            while(current_node->next != nullptr && current_node->key < to_find){
                // Check if the key is already in the data structure
                // if(to_find == current_node->key){
                //     if(current_node->in_use == false){
                //         // Key is already removed
                //         return false;
                //     }else{
                //         return true;
                //     }
                // // Otherwise, move to the next node
                // }else{
                current_node = current_node->next;
                // }
            }

            // If we make it here then we just need to check the node and then return
            if(to_find == current_node->key){
                if(current_node->in_use == false){
                    // Key is already removed
                    threads_working -= 1;
                    return false;
                }else{
                    threads_working -= 1;
                    return true;
                }                
            }

            // Key not in data structure so return
            threads_working -= 1;
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