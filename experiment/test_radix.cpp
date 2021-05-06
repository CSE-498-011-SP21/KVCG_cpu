#include"radix_trie.cpp"
#include<iostream>
#include<bitset>
#include<random>
#include<assert.h>

// Globals are bad... but I don't care
std::default_random_engine generator(42);
std::uniform_int_distribution<int> distribution(0, 4000000);

int get_random_int(){
    return distribution(generator);
}

void p_type(node<int>* n) {
    switch(n->type) {
        case INNER:
            std::cout << "INNER\n";
            break;
        case LEAF:
            std::cout << "LEAF\n";
            break;
    }
}

void p_int(int i) {
    std::cout << std::bitset<8*sizeof(i)>(i) << "\n";
}

void p_tree(node<int>* n) {
    if (n) {
        switch(n->type) {
            case INNER:
            {
                std::cout << "INNER\n";
                p_int(n->key);
                inode<int>* i_n = (inode<int>*) n;
                std::cout << i_n->prefix_len << "\n";
                p_tree(i_n->children[0]);
                p_tree(i_n->children[1]);
                break;
            }
            case LEAF:
                std::cout << "LEAF\n";
                leaf<int, int>* l_n = (leaf<int, int>*) n;
                p_int(n->key);
                p_int(l_n->value);
                break;
        }
    }
}

int main(int argc, char** argv) {
    (void)argv;
    (void)argc;

    radix_trie<int, int> *trie = new radix_trie<int, int>(&get_random_int, &get_random_int);

    int a = 0;
    int b = 0;
    int c = 0;
    int d = 0;

    b = (3 << 30);
    c = (1 << 31);
    d = (1 << 30);

    p_int(a);
    p_int(b);
    p_int(c);
    p_int(d);
    std::cout << "\n";

    trie->add(a, a);
    trie->add(b, b);
    trie->add(c, c);
    trie->add(d, d);
    trie->add(c, 1);

    node<int>* root = trie->get_root();
    p_tree(root);
}
