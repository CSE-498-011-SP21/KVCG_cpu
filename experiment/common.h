/**
 * This fill will define commonly used methods and define an abstract class to base my implementations off of
 */
#ifndef COMMON_H
#define COMMON_H

#include<utility>

// Define a class to implement each version from
template<typename K, typename V>
class cpu_cache{
    // I probably need to declare something with the underlying data structure here
    // OH! I can just do that in each respective class definition! Should I make a null pointer or something though?
    // Like an virtual data point to be defined?

    // TO-DO: Left off here trying to think of a way to make a virtual data point so that each class will be forced to instantiate it
    // TO-DO: Make a target directory to make my file structure cleaner or something? Or a src directory? Might be too much tho...
    // We'll see about these things, there is plenty left to do so just keep setting this up until it works the way you want

    public:
        // Constructor
        // virtual cpu_cache(){}

        // Deconstructor
        virtual ~cpu_cache(){}
        
        // Define virtual methods that must be implemented by each cpu_cache version
        virtual std::pair<bool, bool> add(K key_to_add, V val_to_add) = 0;
        virtual bool remove(K to_remove) = 0;
        virtual bool contains(K to_find) = 0;
        virtual V* range_query(K start, K end) = 0;
        virtual long size() = 0;
        virtual void populate(long num_elements) = 0;

        // Define extra function for printing cause it's my world
        // virtual void printTables() = 0;
};

#endif