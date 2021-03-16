/*
* This file is intended to be used to experiment with linear hashing schemes that could potentially provide for range queries across the hashtable. 
* Original source of code: geeksforgeeks
* -- Amanda Baran
*/

//In KVCG:
// Keys are 8Bs (u64_ints), Vals are pointers or 8B value
//Keys will be 64 bits
//Want to use the first 4 (maybe more?) bits as the prefix
//# to bit: std::string str = std::bitset<64>(K).to_string();
//Bit to #: std::cout << std::bitset<64>("11011011....").to_ulong();

// To use the ordered linear hashing scheme:
// make a mask and apply it and look at the bits to do the stuff with the prefixes.
// hash function is division not mod

//If we don't use an additional structure on top of MICA, it may not be possible to modify for range queries

//TODO: add multithreading

// #include <bits/stdc++.h>

#include <iostream>
#include <vector>
#include <cmath>

//template for generic type
template <typename K, typename V>
class HashNode
{
public:
    V value;
    K key;

    //Constructor of hashnode
    HashNode(K key, V value)
    {
        this->value = value;
        this->key = key;
    }
};

//template for generic type
template <typename K, typename V>
class LinearHashMap
{
    // We use a vector since that is probably optimized for what I am going for (appending to the end and being able to index into any element quickly with underlying memory)
    std::vector<std::vector<HashNode<K,V>>> hash_map;
    // i is the number of bits to consider in the hash function (2^i, that is)
    int i = 0;
    long num_buckets = 0;
    long num_records = 0;
    // p is the value we use to know when we should add a new bucket to the hash map
    // That is, if it is exceded by r/n, then we add a new bucket
    int p = 1;

public:
    LinearHashMap()
    {
        //Initial capacity of hash array
        hash_map = std::vector<std::vector<HashNode<K,V>>>();
        // This reserve is to prevent adding new buckets from being expensive until we increment i
        hash_map.reserve(pow(2, i));
        // Make my first bucket so that there is 1 to start with always
        hash_map.push_back(std::vector<HashNode<K,V>>());
        num_buckets++;
    }

    // This implements hash function to find index for a key
    int hash_code(K key)
    {
        // std::bitset<64> bit_str(key); //makes 64 bit bitset w value of Key
        // bit_str.flip(); //reverses all bits
        //i is the number of bits you are considering --> incrementing i is expensive so we don't want to pick something to small
        //but, we do now the max size of our table so this shouldn't be a huge issue, just need to pick a good i

        return key % (int)pow(2, i);
    }

    //Function to add key value pair
    bool insert_node(K key, V value)
    {
        // TO-DO: Consider duplicates!

        // Apply hash function to find index for given key
        int hash_index = hash_code(key);

        // std::cout << hash_index << std::endl;

        // Now that I have the hash index, I need to insert into the hash map
        // The simple case is I can do just the insertion
        // The edge case is if the bucket I'm indexing into does not exist and I need to get another hash index
        // In addition, there might be an extra special case when the vector is empty so I'm thinking of just initializing everything first to avoid that in the code

        if(hash_index >= num_buckets){
            // This should only ever happen ONCE, if at all
            // That is because there will always be at least 2^(i-1) buckets SO by just ignoring the leading 1, the other bucket should exist
            hash_index /= 2;
        }

        if(hash_index >= num_buckets){
            // For debugging, just check that above assertion is always true
            std::cerr << "Uh-oh! Matt, the hash index is still greater than the number of buckets!" << std::endl;
        }
        
        // Make sure the key is not already in the data structure
        std::vector<HashNode<K,V>> search_bucket = hash_map[hash_index];
        for(unsigned int index=0; index<search_bucket.size(); index++){
            if(key == search_bucket[index].key){
                // Or should I do an update here? That would probably be better, honestly
                return false;
            }
        }

        hash_map[hash_index].push_back(HashNode<K, V>(key, value));
        num_records++;

        // Then, I need to check if p is violated or not and make necessary adjustments!
        double p_check = (double)num_records/num_buckets;
        std::cerr << "P check: " << p_check << std::endl;
        if(p < p_check){
            // p is violated!
            std::cerr << "p was violated, fixing..." << std::endl;

            if(num_buckets == pow(2, i)){
                // We're at the max number of buckets! We need to remake the whole hashmap
                i++;
                num_buckets++;

                // New hash map we are going to use
                std::vector<std::vector<HashNode<K,V>>> new_hash_map = std::vector<std::vector<HashNode<K,V>>>();
                
                // This reserve is to prevent adding new buckets from being expensive until we increment i
                new_hash_map.reserve(pow(2, i));

                // Make new hash map with new buckets
                for(int j=0; j<num_buckets; j++){
                    new_hash_map.push_back(std::vector<HashNode<K,V>>());
                }

                // Rehash old values into new values
                for(auto it=hash_map.begin(); it<hash_map.end(); it++){
                    std::vector<HashNode<K,V>> current_bucket = *it;
                    for(unsigned int index=0; index<current_bucket.size(); index++){
                        int hash_index = hash_code(current_bucket[index].key);
                        if(hash_index >= num_buckets){
                            // This should only ever happen ONCE, if at all
                            // That is because there will always be at least 2^(i-1) buckets SO by just ignoring the leading 1, the other bucket should exist
                            hash_index /= 2;
                        }

                        if(hash_index >= num_buckets){
                            // For debugging, just check that above assertion is always true
                            std::cerr << "Uh-oh! Matt, the hash index is still greater than the number of buckets!" << std::endl;
                        }

                        // We know there are no dups so we don't have to check for that
                        new_hash_map[hash_index].push_back(HashNode<K, V>(current_bucket[index].key, current_bucket[index].value));
                        // Also, num_records has already been incremented so don't worry about that either
                    }
                }

                // Assign the hash_map to the new one that was just made
                hash_map = new_hash_map;
            }else{
                // I can just add a new bucket and remap the other bucket that was being used for all the values in this bucket
                hash_map.push_back(std::vector<HashNode<K,V>>());
                num_buckets++;
                
                // Now, go to other bucket holding some of this bucket's values and add them here, if appropriate
                std::vector<HashNode<K,V>> old_bucket = hash_map[(num_buckets-1)/2];
                std::vector<int> indicies_to_remove_from_old_bucket;
                // Iterate over bucket and remove entries that should be added to the new bucket
                for(unsigned int index=0; index<old_bucket.size(); index++){
                    if(hash_code(old_bucket[index].key) == (num_buckets-1)){
                        // This value entry should be in the new bucket
                        hash_map[num_buckets-1].push_back(old_bucket[index]);
                    }else{
                        // This value entry should be in the old bucket
                        indicies_to_remove_from_old_bucket.push_back(index);
                    }
                }

                // Clean up old bucket
                for(unsigned int index=0; index<indicies_to_remove_from_old_bucket.size(); index++){
                    // Must use the hash_map since old_bucket is a deep copy!
                    // This is super gross, FIX THIS to be more efficient! Just use an iterator on the above loop, comon MAtt
                    hash_map[(num_buckets-1)/2].erase(hash_map[(num_buckets-1)/2].begin() + indicies_to_remove_from_old_bucket[index]);
                }
            }
        }

        return true;

        // hash_map.push_back(HashNode<K, V>(key, value));
    }

    // //Function to search the value for a given key
    V get(int key)
    {
        // Apply hash function to find index for given key
        int hash_index = hash_code(key);

        //finding the node with given key
        if(hash_index < num_buckets){
            std::vector<HashNode<K,V>> search_bucket = hash_map[hash_index];
            for(unsigned int index=0; index<search_bucket.size(); index++){
                if(key == search_bucket[index].key){
                    return search_bucket[index].value;
                }
            }
        }else{
            // In this case, I need to adjust the hash_index
        }

        //If not found return false
        // I'm just going to leave this for now...
        // HOWEVER! TO-DO: How can I more cleverly return when the type is generic V?
        // Dumb question maybe, but I don't remember any clever solutions
        return false;
    }

    // //Function to delete a key value pair
    // V deleteNode(int key)
    // {
    //     // Apply hash function to find index for given key
    //     int hash_index = hash_code(key);

    //     //finding the node with given key
    //     while (arr[hash_index] != nullptr)
    //     {
    //         //if node found
    //         if (arr[hash_index]->key == key)
    //         {
    //             HashNode<K, V> *temp = arr[hash_index];

    //             //Insert dummy node here for further use
    //             arr[hash_index] = dummy;

    //             // Reduce size
    //             size--;
    //             return temp->value;
    //         }
    //         hash_index++;
    //         hash_index %= capacity;
    //     }

    //     //If not found return false
    //     return false;
    // }

    //Return current size
    // int sizeofMap()
    // {
    //     return size;
    // }

    //Return true if size is 0
    // bool isEmpty()
    // {
    //     return size == 0;
    // }

    //Function to display the stored key value pairs
    void display()
    {
        int counter = 0;
        for(auto it=hash_map.begin(); it<hash_map.end(); it++){
            std::vector<HashNode<K,V>> current_bucket = *it;
            std::cout << "Bucket " << counter << ": ";
            for(unsigned int index=0; index<current_bucket.size(); index++){
                std::cout << current_bucket[index].key << ", " << current_bucket[index].value << "; ";
            }
            std::cout << std::endl;
            counter++;
        }
    }
};

//Driver method to test map class
int main()
{
    std::cout << "TEST" << std::endl;
    LinearHashMap<int, int> *mine = new LinearHashMap<int, int>;

    mine->insert_node(10, 20);
    mine->display();
    mine->insert_node(10, 20);
    mine->insert_node(15, 20);
    mine->insert_node(20, 20);
    mine->display();
    // std::cout << mine->get(10) << std::endl;

    // LinearHashMap<int, int> *h = new LinearHashMap<int, int>;
    // h->insert_node(1, 1);
    // h->insert_node(2, 2);
    // h->insert_node(2, 3);
    // h->display();
    // std::cout << h->sizeofMap() << std::endl;
    // std::cout << h->deleteNode(2) << std::endl;
    // std::cout << h->sizeofMap() << std::endl;
    // std::cout << h->isEmpty() << std::endl;
    // std::cout << h->get(2) << std::endl;

    return 0;
}