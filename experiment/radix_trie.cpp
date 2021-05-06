#include "common.h"
#include "node.h"
#include<vector>
#include<iostream>
#include<bitset>

template <typename K, typename V>
class radix_trie: public cpu_cache<K, V> {
    private:
        node<K>* root;
        long trie_size;

        const int k_bits = sizeof(K) * 8;
        
        // Function pointers for key generation
        K (*get_random_K)();
        V (*get_random_V)();

    public:
        // Constructor
        radix_trie(K (*get_random_K_arg)(), V (*get_random_V_arg)()) {
            // Pass key generation functions
            get_random_K = get_random_K_arg;
            get_random_V = get_random_V_arg;
            root = NULL;
        }

        std::pair<bool, bool> add(K key_to_add, V val_to_add) {
            return add_recursive(root, &root, key_to_add, val_to_add);
        }

        std::pair<bool, bool> add_recursive(node<K>* n, node<K>** ref, K key, V val) {
            // Reached a NULL node, insert a leaf at this ref
            if (!n) {
                *ref = (node<K>*) new_leaf(key, val);
                trie_size++;
                return std::make_pair(true, true);
            }

            int gcp = greatest_commmon_prefix(key, n->key);

            // Otherwise, we're either at a leaf or an inner node
            if (n->type == LEAF) {
                // Are we at a duplicate leaf? If so, return success with no change in trie size
                if (n->key == key) {
                    leaf<K, V>* l_n= (leaf<K, V>*) n;
                    l_n->value = val;
                    return std::make_pair(true, false);
                }

                // Split this leaf at the prefix and replace with an inode
                *ref = (node<K>*) inode_split(n, gcp, key, val);
                trie_size++;
                return std::make_pair(true, true);
            } 
            // We're at an inner node, or something has gone horribly wrong
            if (n->type == INNER) {
                inode<K>* i_n = (inode<K>*) n;
                // If we have less than a full prefix match, split inode at the prefix and replace with a new inode
                if (gcp < i_n->prefix_len) {
                    *ref = (node<K>*) inode_split(n, gcp, key, val);
                    trie_size++;
                    return std::make_pair(true, true);
                }
                // Our key shares the entire prefix, recurse to the child on the appropriate side
                node<K>* next_n = i_n->children[get_nth_bit(key, i_n->prefix_len)];
                return add_recursive(next_n, &(i_n->children[get_nth_bit(key, i_n->prefix_len)]), key, val);
            } else {
                return std::make_pair(false, false);
            }
        }

        bool remove(K key_to_remove) {
            return remove_recursive(root, &root, key_to_remove);
        }

        bool remove_recursive(node<K>* n, node<K>** ref, K key) {
            // We're done
            if (!n) {
                return false;
            }

            // We've reached a leaf, just check for a match
            if (n->type == LEAF) {
                if (key == n->key) {
                    *ref = NULL;
                    trie_size--;
                    return true;
                }
            }
            // We're at an inner node, or something has gone horribly wrong
            if (n->type == INNER) {
                inode<K>* i_n = (inode<K>*) n;
                int gcp = greatest_commmon_prefix(key, n->key);
                // If we have less than a full prefix match, the key isn't in the trie
                if (gcp < i_n->prefix_len) {
                    return false;
                }
                // Our key shares the entire prefix, recurse to the child on the appropriate side
                node<K>* next_n = i_n->children[get_nth_bit(key, i_n->prefix_len)];
                return remove_recursive(next_n, &(i_n->children[get_nth_bit(key, i_n->prefix_len)]), key);
            }
            else {
                return false;
            }
        }

        bool contains(K key_to_find) {
            return contains_recursive(root, key_to_find);
        }

        bool contains_recursive(node<K>* n, K key) {
            // We're done
            if (!n) {
                return false;
            }

            // We've reached a leaf, just check for a match
            if (n->type == LEAF) {
                if (key == n->key) {
                    return true;
                }
            } 
            // We're at an inner node, or something has gone horribly wrong
            if (n->type == INNER) {
                inode<K>* i_n = (inode<K>*) n;
                int gcp = greatest_commmon_prefix(key, n->key);
                // If we have less than a full prefix match, the key isn't in the trie
                if (gcp < i_n->prefix_len) {
                    return false;
                }
                // Our key shares the entire prefix, recurse to the child on the appropriate side
                node<K>* next_n = i_n->children[get_nth_bit(key, i_n->prefix_len)];
                return contains_recursive(next_n, key);
            } else {
                return false;
            }
        }
        
        std::vector<V> range_query(K start, K end) {
            return range_query_recursive(root, start, end);
        }

        std::vector<V> range_query_recursive(node<K>* n, K start, K end) {
            // Reached a null point, return nothing
            if (!n) {
                std::vector<V> key_vec;
                return key_vec;
            }
            // If we're at a leaf, return the value if it fits
            if (n->type == LEAF) {
                if (start <= n->key && n->key <= end) {
                    std::vector<V> key_vec;
                    key_vec.push_back(n->key);
                    return key_vec;
                }
            }
            // We're at an inner node, evaluate the branches for range query
            if (n->type == INNER) {
                inode<K>* i_n = (inode<K>*) n;
                if (inverse_mask_n_bits(n->key, i_n->prefix_len) <= end && mask_n_bits(n->key, i_n->prefix_len) >= start) {
                    std::vector<K> v_l = range_query_recursive(i_n->children[0], start, end);
                    std::vector<K> v_r = range_query_recursive(i_n->children[1], start, end);
                    std::vector<K> combined;
                    combined.reserve(v_l.size() + v_r.size());
                    combined.insert(combined.end(), v_l.begin(), v_l.end());
                    combined.insert(combined.end(), v_r.begin(), v_r.end());
                    return combined;
                }
            }
            std::vector<V> key_vec;
            return key_vec;
        }

        long size() {
            return trie_size;
        }

        void populate(long num_elements){
            for (long i = 0; i < num_elements; i++){
                // Ensures effective adds
                while(!add(get_random_K(), get_random_V()).second);
            }
        }

        static int get_nth_bit(K key, int n) {
            return (key >> (((sizeof(K) * 8) - 1) - n)) & 1;
        }

        int greatest_commmon_prefix(K key, K prefix) {
            int i, j, k = 0;
            int bit_len = (int)(sizeof(K) * 8);
            for (i = 0; i < bit_len; i++) {
                j = (key >> ((bit_len - 1) - i)) & 1;
                k = (prefix >> ((bit_len - 1) - i)) & 1;

                if (j != k) {
                    return i;
                }
            }
            return i;
        }

        static node<K>* new_node(char type, K key) {
            node<K>* n;
            switch(type) {
                case INNER:
                    n = (node<K>*) new inode<K>();
                    break;
                case LEAF:
                    n = (node<K>*) new leaf<K, V>();
                    break;
            }
            n->type = type;
            n->key = key;
            return n;
        }

        static leaf<K, V>* new_leaf(K key, V value) {
            leaf<K, V>* l = (leaf<K, V>*) new_node(LEAF, key);
            l->value = value;
            return l;
        }

        static inode<K>* new_inode(K key, int prefix_len, node<K>* l, node<K>* r) {
            inode<K>* i = (inode<K>*) new_node(INNER, key);
            i->prefix_len = prefix_len;
            i->children[0] = l;
            i->children[1] = r;
            return i;
        }

        static inode<K>* inode_split(node<K>* n, int gcp, K key, V val) {
            leaf<K, V>* l = new_leaf(key, val);
            inode<K>* i;

            // Determine whether to insert on the left or right
            if (get_nth_bit(key, gcp)) { // Differing bit for key = 1, n->key = 0
                i = new_inode(key, gcp, n, (node<K>*) l);
            } else {
                i = new_inode(key, gcp, (node<K>*) l, n);
            }
            return i;
        }
        
        K inverse_mask_n_bits(K key, int n) {
            int mask = ~((1 << n) - 1);
            return (key & mask);
        }

        K mask_n_bits(K key, int n) {
            int mask = ((1 << n) - 1);
            return (key & mask);
        }

        node<K>* get_root() {
            return root;
        }

        void p_int(int i) {
            std::cout << std::bitset<8*sizeof(i)>(i) << "\n";
        }
};
