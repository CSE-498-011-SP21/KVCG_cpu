#include "common.h"
#include "art/Tree.h"
#include<vector>
#include<functional>
#include<iostream>

using namespace std;

template <typename K, typename V>
class art: public cpu_cache<K, V> {

    private:
        ART_OLC::Tree tree;

        K (*get_random_K)();
        V (*get_random_V)();

    public:
        // Constructor
        art(LoadKeyFunction loadKey, K (*get_random_K_arg)(), V (*get_random_V_arg)()) {
            tree = ART_OLC::Tree tree(loadKey)
        }

        ~art() {
            delete tree
        }
        
        std::pair<bool, bool> add(K key_to_add, V val_to_add){
            auto t = tree.getThreadInfo();

            Key key;
            loadKey(key_to_add, key);
            tree.insert(key, key_to_add, t);

            return std::make_pair(true, true);
        }

        bool remove(K to_remove){
            auto t = tree.getThreadInfo();

            Key key;
            loadKey(to_remove, key);
            tree.remove(key, to_remove, t)

            return std::make_pair(true, true);
        }

        bool contains(K to_find){
            auto t = tree.getThreadInfo();

            Key key;
            loadKey(to_find, key);
            auto val = tree.lookup(key, t);

            if (val != to_find) {
                return false;
            }
            return true;
        }

        std::vector<V> range_query(K start, K end){
            auto t = tree.getThreadInfo();

            Key k_start;
            Key k_end;
            loadKey(start, k_start);
            loadKey(end, k_end);

            Key contKey;
            uint64_t result[];

            std::size_t res_size = sizeof(uint64_t);
            std::size_t res_found;

            auto val = tree.lookupRange(k_start, k_end, contKey, result, res_size, res_found, t)

            if (val) {
                return result;
            }
            std::vector<int> empty_vec;
            return empty_vec
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

        void loadKey(TID tid, Key &key) {
            // Store the key of the tuple into the key vector
            // Implementation is database specific
            key.setKeyLen(sizeof(tid));
            reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
        }
};