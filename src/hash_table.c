#include "hash_table.h"
#include <stdio.h>
#include <stdlib.h>
node* head;

node* insert_head(node* cur_head, node* n) {
    node* tmp = cur_head;

    n->next = tmp;
    cur_head = n;

    return cur_head;
    
    
}

// void remove(node* n) {
//     if (n->next)
//         n->next->prev = n->prev;
//     if (n->prev)
//         n->prev->next = n->next;
//     else
//         head = n->next;
// }

// unsigned int hash(unsigned int s)
//     //hash(unsigned char *str)
//     {
//         unsigned long hash = 5381;
//         int c;

        
//         hash = ((hash << 5) + hash) + s; /* hash * 33 + c */

//         return hash % 71008999;
//     }

// stack overflow: https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key

unsigned int hash(unsigned int x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x%1000003;
}
// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    // The next line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    if (*ht) {
        return -1;
    }
    hashtable* h= (hashtable*) malloc(sizeof(hashtable));
    if (!h) {
        return -1;
    }
    //hashtable *h=NULL;
    for (int i = 0; i < 1000003; ++i) {
        h->arr[i] = NULL;
        h->num_res[i] = 0;
    }
    h->num_elt = size;
    *ht = h;

    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put(hashtable* ht, keyType key, valType value) {
    // check if hashtable has been allocated
    if (!ht){
        return -1;
    }

    // adding value so increment counter
    ht->num_elt++;
    unsigned int hash_index = hash((unsigned int) key);

    // create node for new value
    node* n = (node *) malloc(sizeof(node));
    if (!n) {
        return -1;
    }
    n->val = value;
    n->key = key;
    
    // check if values already exist in hash table
    if (!(ht->arr[hash_index])) {
        ht->arr[hash_index] = n;
        n->next = NULL;
    }  else {
        ht->arr[hash_index] = insert_head(ht->arr[hash_index], n);
    }
    ht->num_res[hash_index]++;

    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
    // check hashtable has been allocated
    if (!ht) {
        return -1;
    }
    *num_results = 0;
    if (num_values == 0) {
        return 0;
    }
    // check if values exist in hashtable
    unsigned int hash_index = hash((unsigned int) key);
    if (!(ht->arr[hash_index])) {
        return -1;
    }

    // get as many values as possible (until reach null pointer or 
    // num_results >= num_values)
    node* head = ht->arr[hash_index];
    while (((*num_results) != num_values) && head) {
        if (head->key == key) {
            //printf("FOUND MATCHING VAL IN HT.C\n");
            values[*num_results] = (valType) head->val;
            (*num_results)++;
        }

        head = head->next;
        
    }

    if (*num_results == 0) {
        printf("yuh1");
    }

    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    if (!ht) {
        return -1;
    }

    unsigned int hash_key = hash((unsigned int) key);
    node * head = ht->arr[hash_key];
    if (head) {
        while (head) {
            node* temp = head;
            head = head->next;
            free(temp);
            ht->num_elt--;
        }
        ht->arr[hash_key] = NULL;
    }

    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.

    for (int i = 0; i != 10067; ++i) {
        node* h = ht->arr[i];

        while(h) {
            node* temp = h;
            h = h->next;
            free(temp);
        }

        //free(ht->arr[i]);     

    }
    free(ht);
  
    return 0;
}
