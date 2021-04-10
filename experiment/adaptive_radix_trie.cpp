#include<vector>
#include<functional>
#include<iostream>
#include "common.h"
#include "art/Tree.h"

void loadKey(TID tid, Key &key) {
        // Store the key of the tuple into the key vector
        // Implementation is database specific
        key.setKeyLen(sizeof(tid));
        reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
};

template <typename K, typename V>
class adaptive_radix_trie: public cpu_cache<K, V> {

    private:
        ART_OLC::Tree art;

        K (*get_random_K)();
        V (*get_random_V)();

    public:

        // Constructor
        adaptive_radix_trie(K (*get_random_K_arg)(), V (*get_random_V_arg)()) : art(loadKey) {                
            get_random_K = get_random_K_arg;
            get_random_V = get_random_V_arg;
        }
        
        std::pair<bool, bool> add(K key_to_add, V){
            auto t = art.getThreadInfo();

            uint64_t cast_to_add = (unsigned long) key_to_add;
            Key key;
            loadKey(cast_to_add, key);
            art.insert(key, cast_to_add, t);

            return std::make_pair(true, true);
        }

        bool remove(K to_remove){
            auto t = art.getThreadInfo();

            Key key;
            loadKey(to_remove, key);
            art.remove(key, to_remove, t);

            return true;
        }

        bool contains(K to_find){
            auto t = art.getThreadInfo();

            Key key;
            loadKey(to_find, key);
            auto val = art.lookup(key, t);

            if (val != (unsigned long)to_find) {
                return false;
            }
            return true;
        }

        std::vector<V> range_query(K start, K end){
            auto t = art.getThreadInfo();

            Key k_start;
            Key k_end;
            loadKey(start, k_start);
            loadKey(end, k_end);

            Key contKey;
            uint64_t result[0];

            std::size_t res_size = sizeof(uint64_t);
            std::size_t res_found;

            auto val = art.lookupRange(k_start, k_end, contKey, result, res_size, res_found, t);

            if (val) {
                int n = sizeof(result) / sizeof(result[0]);
                std::vector<int> vec_result(result, result + n);
                return vec_result;
            }
            std::vector<int> empty_vec;
            return empty_vec;
        }

        long size(){
            long trie_size = 0;

            return trie_size;
        }

        void populate(long num_elements){
            for(long i=0; i<num_elements; i++){
                // Ensures effective adds
                while(!add(get_random_K(), get_random_V()).second);
            }
        }
};    