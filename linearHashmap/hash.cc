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


#include<bits/stdc++.h> 
using namespace std; 
  
//template for generic type 
template<typename K, typename V> 
  
//Hashnode class 
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
template<typename K, typename V> 
  
//Our own Hashmap class 
class HashMap 
{ 
    //hash element array 
    HashNode<K,V> **arr; 
    int capacity; 
    //current size 
    int size; 
    //dummy node 
    HashNode<K,V> *dummy; 
  
    public: 
    HashMap() 
    { 
        //Initial capacity of hash array 
        capacity = 100; 
        size=0; 
        arr = new HashNode<K,V>*[capacity]; 
          
        //Initialise all elements of array as NULL 
        for(int i=0 ; i < capacity ; i++) 
            arr[i] = NULL; 
          
        //dummy node with value and key -1 
        dummy = new HashNode<K,V>(-1, -1); 
    } 

    // This implements hash function to find index for a key 
    int hashCode(K key) 
    { 
        std::bitset<64> bit_str(key); //makes 64 bit bitset w value of Key
        bit_str.flip(); //reverses all bits
        //i is the number of bits you are considering --> incrementing i is expensive so we don't want to pick something to small   
        //but, we do now the max size of our table so this shouldn't be a huge issue, just need to pick a good i     
        
        //return key % capacity; 
    } 
      
    //Function to add key value pair 
    void insertNode(K key, V value) 
    {
        HashNode<K,V> *temp = new HashNode<K,V>(key, value); 
          
        // Apply hash function to find index for given key 
        int hashIndex = hashCode(key); 
          
        //find next free space  
        while(arr[hashIndex] != NULL && arr[hashIndex]->key != key 
               && arr[hashIndex]->key != -1) 
        { 
            hashIndex++; 
            hashIndex %= capacity; 
        } 
          
        //if new node to be inserted increase the current size 
        if(arr[hashIndex] == NULL || arr[hashIndex]->key == -1) 
            size++; 
        arr[hashIndex] = temp; 
    } 
      
    //Function to delete a key value pair 
    V deleteNode(int key) 
    { 
        // Apply hash function to find index for given key 
        int hashIndex = hashCode(key); 
          
        //finding the node with given key 
        while(arr[hashIndex] != NULL) 
        { 
            //if node found 
            if(arr[hashIndex]->key == key) 
            { 
                HashNode<K,V> *temp = arr[hashIndex]; 
                  
                //Insert dummy node here for further use 
                arr[hashIndex] = dummy; 
                  
                // Reduce size 
                size--; 
                return temp->value; 
            } 
            hashIndex++; 
            hashIndex %= capacity; 
  
        } 
          
        //If not found return null 
        return NULL; 
    } 
      
    //Function to search the value for a given key 
    V get(int key) 
    { 
        // Apply hash function to find index for given key 
        int hashIndex = hashCode(key); 
        int counter=0; 
        //finding the node with given key    
        while(arr[hashIndex] != NULL) 
        {    int counter =0; 
             if(counter++>capacity)  //to avoid infinite loop 
                return NULL;         
            //if node found return its value 
            if(arr[hashIndex]->key == key) 
                return arr[hashIndex]->value; 
            hashIndex++; 
            hashIndex %= capacity; 
        } 
          
        //If not found return null 
        return NULL; 
    } 
      
    //Return current size  
    int sizeofMap() 
    { 
        return size; 
    } 
      
    //Return true if size is 0 
    bool isEmpty() 
    { 
        return size == 0; 
    } 
      
    //Function to display the stored key value pairs 
    void display() 
    { 
        for(int i=0 ; i<capacity ; i++) 
        { 
            if(arr[i] != NULL && arr[i]->key != -1) 
                cout << "key = " << arr[i]->key  
                     <<"  value = "<< arr[i]->value << endl; 
        } 
    } 
}; 
  
//Driver method to test map class 
int main() 
{ 
    HashMap<int, int> *h = new HashMap<int, int>; 
    h->insertNode(1,1); 
    h->insertNode(2,2); 
    h->insertNode(2,3); 
    h->display(); 
    cout << h->sizeofMap() <<endl; 
    cout << h->deleteNode(2) << endl; 
    cout << h->sizeofMap() <<endl; 
    cout << h->isEmpty() << endl; 
    cout << h->get(2); 
  
    return 0; 
} 