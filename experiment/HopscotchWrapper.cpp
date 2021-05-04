#include <stdio.h>
#include <limits.h>
#include <sys/types.h>
#include <malloc.h>
#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <alloca.h>
#include <strings.h>
#include <string.h>
#include <shared_mutex>
#include <mutex>
#include "common.h"
#include <vector>
#include <functional>
#include <iostream>
#include <math.h>
#include <atomic>
#include "hopscotch_v2.cpp"
//CLASS HopscotchWrapper

template <typename K,typename V>
class HopscotchWrapper: public cpu_cache<K, V>{

    private:
    
    HopscotchHashMap::HopscotchHashMap<K, V, HASH_INT, std::unique_lock<std::shared_mutex>, Memory> _ds;
    K (*get_random_K)();
    V (*get_random_V)();
    long (*hash0)(K); 

    public:

    HopscotchWrapper( K (*get_random_K_arg)(), V (*get_random_V_arg)(), long (*hash0_arg)(K)){
        get_random_K = get_random_K_arg;
        get_random_V = get_random_V_arg;
        hash0 = hash0_arg;
        _ds = new HopscotchHashMap<K, V, HASH_INT, std::unique_lock<std::shared_mutex>, Memory>(32*1024,16,64,true, hash0);
    }

    ~HopscotchWrapper(){
        delete _ds;
    }


    std::pair<bool, bool> add(K key_to_add, V val_to_add){
        return _ds.putIfAbsent(key_to_add, val_to_add);
    }
   
    bool remove(K to_remove){
        V res = _ds.remove(to_remove);
        if (res == HASH_INT::_EMPTY_DATA){
            return false;
        }
        return true;
    }

    bool contains(K to_find){
       return _ds.containsKey(to_find);
    }

    std::vector<V> range_query(K start, K end){
        std::vector<V> values;
        if(start > end){
            return values;
        }
        values = _ds.range_query(start, end);
        return values;
    }

    long size(){
        unsigned int total_elements = _ds.size();
        return total_elements;
    }

    void populate(long num_elements){
        for(long i=0; i<num_elements; i++){
            // Ensures effective adds
            while(!add(get_random_K(), get_random_V()).second);
        }
    }

    void resize(){
        delete _ds;
        _ds = new HopscotchHashMap<K, V, HASH_INT, std::unique_lock<std::shared_mutex>, Memory>(32*1024*2,16,64,true, hash0);
        return;
        //TODO: ask if this will work, will the return to the put method not use this new ds?
    }
};
