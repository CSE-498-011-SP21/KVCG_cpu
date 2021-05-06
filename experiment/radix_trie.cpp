#include "common.h"
#include "node.h"
#include<vector>

template <typename K, typename V>
class radix_trie: public cpu_cache<K, V> {
    private:
        // Base of tree with null prefix
        node<K>* root;

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

            root = new node*;
        }

        std::pair<bool, bool> add(K key_to_add, V val_to_add) {
            return std::make_pair(true, true);
        }

        std::pair<bool, bool> add_recursive(node<K>* n, node<K>** ref, K key, V val) {
            // Reached a NULL node, insert a leaf at this ref
            if (!n) {
                *ref = (node<K>*) new_leaf(key, val);
                return std::make_pair(true, true);
            }

            int gcp = greatest_commmon_prefix(key, n->key);

            // Otherwise, we're either at a leaf or an inner node
            if (n->type == LEAF) {
                // Are we at a duplicate leaf? If so, return success with no change in trie size
                if (n->key == key) {
                    return std::make_pair(true, false);
                }

                // Split this leaf at the prefix and replace with an inode
                *ref = (node<K>*) inode_split(n, gcp, key, val);
                return std::make_pair(true, true);
            } 
            // We're at an inner node, or something has gone horribly wrong
            if (n->type == INNER) {
                inode<K>* i_n = (inode<K>*) n;
                // If we have less than a full prefix match, split inode at the prefix and replace with a new inode
                if (gcp < i_n->prefix_len) {
                    *ref = (node<K>*) inode_split(n, gcp, key, val);
                    return std::make_pair(true, true);
                }
                // Our key shares the entire prefix, recurse to the child on the appropriate side
                node<K>* next_n = i_n->children[get_nth_bit(key, i_n->prefix_len)];
                return add_recursive(next_n, n, key, val);
            }
        }

        static inode<K>* inode_split(node<K>* n, int gcp, K key, V val) {
            leaf<K, V> l = new_leaf(key, val);
            inode<K>* i;

            // Determine whether to insert on the left or right
            if (get_nth_bit(key, gcp)) { // Differing bit for key = 1, n->key = 0
                i = new_inode(key, gcp, n, (node<K>*) l);
            } else {
                i = new_inode(key, gcp, (node<K>*) l, n);
            }
            return i;
        }

        std::pair<bool, bool> add_internal(K key_to_add, V val_to_add, inner<K>* n, int i) {
            // Get the ith bit of K
            int index_bit = get_nth_bit(key_to_add, i);
            
            if (n->children[index_bit]->type() == node_type::leaf) {
                // Check for duplicate key
                if (key_to_add == next->key) {
                    return std::make_pair(true, false);
                }

                // Else, find the shared prefix between keys
                int gcp = greatest_commmon_prefix(key_to_add, next->key);
               
                inner<K>* new_node = new inner<K>();
                new_node->prefix = key_to_add;
                new_node->prefix_len = (gcp - 1);

                leaf<K, V>* inserted = new leaf<K, V>();
                inserted->key = key_to_add;
                inserted->value = val_to_add;

                if (next->key > key_to_add) {
                    new_node->children[0] = inserted;
                    new_node->children[1] = next;

                    n->children[index_bit] = new_node;
                } else {
                    new_node->children[0] = next;
                    new_node->children[1] = inserted;

                    n->children[index_bit] = new_node;
                }
                return std::make_pair(true, true);
            }

            int gcp = greatest_commmon_prefix(key_to_add, next->key);

            if (gcp < next->prefix_len) {
                inner<K>* new_node = new inner<K>();
                new_node->prefix = key_to_add;
                new_node->prefix_len = (gcp - 1);

                leaf<K, V>* inserted = new leaf<K, V>();
                inserted->key = key_to_add;
                inserted->value = val_to_add;

                if (next->key > key_to_add) {
                    new_node->children[0] = inserted;
                    new_node->children[1] = next;

                    n->children[index_bit] = new_node;
                } else {
                    new_node->children[0] = next;
                    new_node->children[1] = inserted;

                    n->children[index_bit] = new_node;
                }
                return std::make_pair(true, true);
            } else {
                return add_internal(key_to_add, val_to_add, next, i += (gcp - 1));
            }
        }

        bool remove(K key_to_remove) {
            return true;
        }

        bool remove_internal(K key_to_remove, inner<K>* n, int i) {
            // Get the ith bit of K
            int index_bit = get_nth_bit(key_to_remove, i);

            inner<K>* next = n->children[index_bit];

            // If we're dealing with a brand new node
            if (!next) {
                return false;
            }
        }

        int get_nth_bit(K key, int n) {
            return (key >> ((sizeof(K) * 8) - 1) - n) & 1;
        }

        int greatest_commmon_prefix(K key, K prefix) {
            int len = sizeof(K) * 8;
            int j, k;

            for (int i = 0; i < len; i++) {
                j = (key >> ((len - 1) - i)) & 1;
                k = (prefix >> ((len - 1) - i)) & 1;

                if (j != k) {
                    return i;
                }
            }
            return 0;
        }

        static node<K>* new_node(uint8_t type, K key) {
            node<K>* n;
            switch(type) {
                case INNER:
                    n = (node<K>*) new inode<K>();
                    break;
                case LEAF:
                    n = (node<K>*) new leaf<K, V>();
                    break;
                default:
                    abort();
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
};
