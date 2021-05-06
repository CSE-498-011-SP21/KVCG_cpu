#include<vector>

#define INNER   0
#define LEAF    1

template<typename K>
struct node {
    char type;
    K key;
};

template<typename K>
struct inode {
    node<K> n;
    int prefix_len;
    node<K>* children[2];
};

template<typename K, typename V>
struct leaf {
    node<K> n;
    V value; 
};