#include "common.h"
#include<vector>
#include<functional>
#include<iostream>

// TO-DO: Deal with problem when hash functions return a value that is smaller than the mod and we keep resizing!
// Maybe just use a new hash function or apply a salt or something?

template <typename T>
class sequential: public hash_set<T>{

    struct hash_entry{
        T value;
        bool in_use;
    };

    private:
        long table_cap;

        int add_limit;

        hash_entry* table_0;
        hash_entry* table_1;

        // Declare function pointers
        long (*hash0)(T);
        long (*hash1)(T);
        T (*get_random_T)();

        void resize(){
            long old_size = table_cap;
            table_cap = (table_cap*2) + 1;

            // If I want to be able to re-use the functions in this class, I need to reassign table_0 and table_1
            hash_entry* old_table_0 = table_0;
            hash_entry* old_table_1 = table_1;

            table_0 = new hash_entry[table_cap];
            table_1 = new hash_entry[table_cap];

            // Add things back in the order they were added is probably best so start with table_1
            for(long i=0; i<old_size; i++){
                if(old_table_1[i].in_use){
                    add(old_table_1[i].value);
                }
            }

            for(long i=0; i<old_size; i++){
                if(old_table_0[i].in_use){
                    add(old_table_0[i].value);
                }
            }

            // Free the old memory
            delete old_table_0;
            delete old_table_1;
        }

    public:
        // Constructor
        sequential(long init_cap, int init_add_limit, long (*hash0_arg)(T), long (*hash1_arg)(T), T (*get_random_T_arg)()){
            table_cap = init_cap;
            add_limit = init_add_limit;
            hash0 = hash0_arg;
            hash1 = hash1_arg;
            get_random_T = get_random_T_arg;

            // Allocate memory, but it's garbage
            table_0 = new hash_entry[table_cap];
            table_1 = new hash_entry[table_cap];

            // Mark all entries in both tables as not in use
            for(long i=0; i<table_cap; i++){
                table_0[i].in_use = false;
            }

            for(long i=0; i<table_cap; i++){
                table_1[i].in_use = false;
            }
        }

        // Default init_add_limit to 1
        sequential(long init_cap, long (*hash0_arg)(T), long (*hash1_arg)(T), T (*get_random_T_arg)()) : sequential(init_cap, 1, hash0_arg, hash1_arg, get_random_T_arg){}

        // Deconstructor
        ~sequential(){
            delete table_0;
            delete table_1;
        }
        
        bool add(T to_add){
            if(contains(to_add)){
                return false;
            }

            for(int i=0; i<add_limit; i++){
                long hashed_value_0 = hash0(to_add);
                long index_0 = hashed_value_0 % table_cap;

                // First, check if the first table contains a value already
                if(!table_0[index_0].in_use){
                    // If not, add our value to the structure and return
                    table_0[index_0].value = to_add;
                    table_0[index_0].in_use = true;
                    return true;
                }else{
                    // If we're here, we need to swap the value in table_0 with the value in table_1
                    T save_add = table_0[index_0].value;
                    table_0[index_0].value = to_add;
                    // Overwrite to_add so in the edge case that we resize, the saved add value is not lost (and the other one was already swapped in so it's good)
                    to_add = save_add;

                    long hashed_value_1 = hash1(to_add);
                    long index_1 = hashed_value_1 % table_cap;
                    
                    // Now we check if we can add the next value 
                    if(!table_1[index_1].in_use){
                        // We can add the value
                        table_1[index_1].value = to_add;
                        table_1[index_1].in_use = true;
                        return true;
                    }else{
                        save_add = table_1[index_1].value;
                        table_1[index_1].value = to_add;
                        to_add = save_add;
                        // Now we retry from the beginning of the for loop
                    }
                }
            }

            // If we make it here, we have to resize the data structure
            resize();
            add(to_add);

            return true;
        }

        bool remove(T to_remove){
            long hashed_value_0 = hash0(to_remove);
            long index_0 = hashed_value_0 % table_cap;

            // First, check if the first table contains the hashed value
            if(table_0[index_0].in_use){
                if(table_0[index_0].value == to_remove){
                    // Found it! Mark it as not in use
                    table_0[index_0].in_use = false;
                    return true;
                }
            }

            long hashed_value_1 = hash1(to_remove);
            long index_1 = hashed_value_1 % table_cap;
            
            if(table_1[index_1].in_use){
                if(table_1[index_1].value == to_remove){
                    // Found it! Mark it as not in use
                    table_1[index_1].in_use = false;
                    return true;
                }
            }

            return false;
        }

        bool contains(T to_find){
            long hashed_value_0 = hash0(to_find);
            long index_0 = hashed_value_0 % table_cap;

            // First, check if the first table contains the hashed value
            if(table_0[index_0].in_use){
                if(table_0[index_0].value == to_find){
                    return true;
                }
            }

            long hashed_value_1 = hash1(to_find);
            long index_1 = hashed_value_1 % table_cap;
            
            if(table_1[index_1].in_use){
                if(table_1[index_1].value == to_find){
                    return true;
                }
            }

            return false;
        }

        long size(){
            long total_elements = 0;
            
            for(long i=0; i<table_cap; i++){
                if(table_0[i].in_use == true){
                    total_elements++;
                }
            }

            for(long i=0; i<table_cap; i++){
                if(table_1[i].in_use == true){
                    total_elements++;
                }
            }

            return total_elements;
        }

        void populate(long num_elements){
            for(long i=0; i<num_elements; i++){
                // Ensures effective adds
                while(!add(get_random_T()));
            }
        }

        void printTables(){
            std::cout << "Table Sizes: " << table_cap << std::endl;

            // Should I have a to string function or something for printing the T values?
            std::cout << "Table 0" << std::endl;
            for(long i=0; i<table_cap; i++){
                std::cout << "Index: " << i << ", value: " << table_0[i].value << ", in_use: " << table_0[i].in_use << std::endl; 
            }

            std::cout << "Table 1" << std::endl;
            for(long i=0; i<table_cap; i++){
                std::cout << "Index: " << i << ", value: " << table_1[i].value << ", in_use: " << table_1[i].in_use << std::endl; 
            }
        }
};