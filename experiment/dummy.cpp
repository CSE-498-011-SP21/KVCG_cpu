#include "common.h"
#include<vector>
#include<functional>
#include<iostream>

// TO-DO: Deal with problem when hash functions return a value that is smaller than the mod and we keep resizing!
// Maybe just use a new hash function or apply a salt or something?

// LEFT-OFF:
/*
    1. Still need to implement range query (to do that, I need to fix the hash function to be sequential)
    2. Implement range query testing in test bed
    3. Write script to get results from test bed (and maybe change the .h a little to be more representative of what we need, like returning V, not just a bool on a contains)
    4. Make this concurrent
    I want to make a note that with an allowed chain length of 1, this is at it's fastest. 
    I wonder if that's okay, to just make a huge hash map. Well, that's why the dynamically sized hash map could be cool.
    HOWEVER, does that guarantee one element per bucket? If it does, it's better. If not, then it isn't. 
    Also, is one hop enough? Or do I need multiple hops, like I would end up in one place and then be forced to go to another until I found what I was looking for?
    This scheme here could be find... if there is a way to compress the empty buckets (since no more indicies will be added after the model tells us everything)
    I could maybe consolidate the ranges (for example, if buckets 700-900 are all free, then just note that somewhere and don't include them in the final bucket array? Might cost a little extra time though... hmmmmm)
    5. Also, maybe deal with that thing where I return false on successful updates since it doesn't change the size of the data structure
*/

template <typename K, typename V>
class traditional_hash_map: public cpu_cache<K, V>{

    struct hash_entry{
        K key;
        V value;
        bool in_use;
        hash_entry* next;
    };

    private:
        long num_buckets;

        int chain_length_max;

        hash_entry* table;

        // Declare function pointers
        long (*hash0)(K);
        K (*get_random_K)();
        V (*get_random_V)();

        void resize(){
            // std::cerr << "IN RESIZE, Num Buckets: " << num_buckets << std::endl;
            long original_size = size();

            hash_entry* old_table = table;
            long num_buckets_old = num_buckets;
            table = new hash_entry[num_buckets*2];
            num_buckets *= 2;

            // Iterate over all buckets
            for(int i=0; i<num_buckets_old; i++){
                hash_entry* current_node = &old_table[i];

                while(current_node->next != nullptr){
                    // Check if the node is being used
                    if(current_node->in_use == true){
                        add(current_node->key, current_node->value);
                    }
                    hash_entry* next_node = current_node->next;
                    // delete current_node;
                    current_node = next_node;
                }

                // Check last node
                if(current_node->in_use == true){
                    add(current_node->key, current_node->value);
                }
                // delete current_node;
            }

            if(original_size != size()){
                std::cout << "ORIGINAL SIZE: " << original_size << std::endl;
                std::cout << "WRONG" << std::endl;
                std::cout << "New size: " << size() << std::endl;
            }

            // delete old_table;

            // LEFT-OFF HERE: SOMETHING IS WRONG WITH THIS FUNCTION
            // This doesn't make any sense. I have to call it to do other work...
            // Basically, for some reason, whenever I try to add 9042293, it thinks the key is already in the table when it definitely is not
            // I literally have no idea what's going on... this makes no sense
        }

    public:
        // Constructor
        traditional_hash_map(long init_buckets, int init_chain_length, long (*hash0_arg)(K), K (*get_random_K_arg)(), V (*get_random_V_arg)()){
            num_buckets = init_buckets;
            chain_length_max = init_chain_length;
            hash0 = hash0_arg;
            get_random_K = get_random_K_arg;
            get_random_V = get_random_V_arg;

            // Allocate memory, but it's garbage
            table = new hash_entry[num_buckets];

            // Set all linked lists to null pointers to begin
            for(long i=0; i<num_buckets; i++){
                table[i].in_use = false;
                table[i].next = nullptr;
            }
        }

        // Default init_add_limit to 1
        traditional_hash_map(long init_buckets, long (*hash0_arg)(K), K (*get_random_K_arg)(), V (*get_random_V_arg)()) : traditional_hash_map(init_buckets, 1, hash0_arg, get_random_K_arg, get_random_V_arg){}

        // Deconstructor
        ~traditional_hash_map(){
            // printTables();
            
            for(int i=0; i<num_buckets; i++){
                // std::cerr << "Index i: " << i << std::endl;

                hash_entry* current_node = &table[i];
                hash_entry* next_node = current_node->next;

                while(next_node != nullptr){
                    // std::cerr << "In while: " << i << std::endl;
                    // Check if the node is being used
                    // delete current_node;
                    current_node = next_node;
                    next_node = current_node->next;
                }

                // delete current_node;
            }
            // delete table;
        }
        
        std::pair<bool, bool> add(K key_to_add, V val_to_add){
            // std::cerr << "IN ADD: " << key_to_add << ", " << val_to_add << std::endl;

            long hashed_value = hash0(key_to_add);
            long index = hashed_value % num_buckets;
            
            int linked_list_counter = 0;

            hash_entry* current_node = &table[index];

            while(current_node->next != nullptr){
                // Check if the key is already in the data structure
                if(key_to_add == current_node->key){
                    current_node->value = val_to_add;
                    if(current_node->in_use == true){
                        return std::make_pair(true, false);
                    }
                    current_node->in_use = true;
                    return std::make_pair(true, true);
                // Otherwise, move to the next node
                }else{
                    current_node = current_node->next;
                    linked_list_counter++;
                }
            }

            // Check last node
            if(key_to_add == current_node->key){
                current_node->value = val_to_add;
                if(current_node->in_use == true){
                    return std::make_pair(true, false);
                }
                current_node->in_use = true;
                return std::make_pair(true, true);
            // Otherwise, we're done checking
            }else{
                linked_list_counter++;
            }


            // If we make it here, then the key is not already in the linked list so we need to add it
            // Before that, let's make sure the linked list won't be too long when I add another node
            if(linked_list_counter > chain_length_max){
                // We need to resize
                resize();
                return add(key_to_add, val_to_add);
            }

            // If we make it here, then just add the new item to the linked list
            hash_entry* new_node = new hash_entry;
            new_node->key = key_to_add;
            new_node->value = val_to_add;
            new_node->in_use = true;
            new_node->next = nullptr;
            current_node->next = new_node;

            return std::make_pair(true, true);
        }

        bool remove(K to_remove){
            // std::cerr << "IN REMOVE: " << to_remove << std::endl;

            long hashed_value = hash0(to_remove);
            long index = hashed_value % num_buckets;

            hash_entry* current_node = &table[index];

            while(current_node->next != nullptr){
                // Check if the key is already in the data structure
                if(to_remove == current_node->key){
                    if(current_node->in_use == false){
                        // Key is already removed
                        return false;
                    }else{
                        current_node->in_use = false;
                        return true;
                    }
                // Otherwise, move to the next node
                }else{
                    current_node = current_node->next;
                }
            }

            // If we make it here then we just need to check the last node and then return
            if(to_remove == current_node->key){
                if(current_node->in_use == false){
                    // Key is already removed
                    return false;
                }else{
                    current_node->in_use = false;
                    return true;
                }                
            }

            // Key not in data structure so return
            return false;
        }

        bool contains(K to_find){
            // std::cerr << "IN CONTAINS: " << to_find << std::endl;

            long hashed_value = hash0(to_find);
            long index = hashed_value % num_buckets;

            hash_entry* current_node = &table[index];

            while(current_node->next != nullptr){
                // Check if the key is already in the data structure
                if(to_find == current_node->key){
                    if(current_node->in_use == false){
                        // Key is already removed
                        return false;
                    }else{
                        return true;
                    }
                // Otherwise, move to the next node
                }else{
                    current_node = current_node->next;
                }
            }

            // If we make it here then we just need to check the last node and then return
            if(to_find == current_node->key){
                if(current_node->in_use == false){
                    // Key is already removed
                    return false;
                }else{
                    return true;
                }                
            }

            // Key not in data structure so return
            return false;
        }

        V* range_query(K start, K end){
            if(start == end){
                return nullptr;
            }
            return nullptr;
        }

        long size(){
            // std::cerr << "IN SIZE" << std::endl;

            long total_elements = 0;
            
            for(int i=0; i<num_buckets; i++){
                hash_entry* current_node = &table[i];

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
        }

        void printTables(){
            std::cout << "Table Size: " << num_buckets << std::endl;

            for(int i=0; i<num_buckets; i++){
                hash_entry* current_node = &table[i];

                std::cout << "Table " << i << ": ";

                while(current_node->next != nullptr){
                    // Check if the node is being used
                    if(current_node->in_use == true){
                        std::cout << current_node->key << ", " << current_node->value;
                    }
                    current_node = current_node->next;
                }

                // Check last node
                if(current_node->in_use == true){
                    std::cout << current_node->key << ", " << current_node->value;
                }
                
                std::cout << std::endl;
            }
        }
};