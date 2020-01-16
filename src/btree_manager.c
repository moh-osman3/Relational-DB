#define _DEFAULT_SOURCE
#include "cs165_api.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "client_context.h"
#include "utils.h"
#include "parse.h"
#include <limits.h>
#include "hash_table.h"


// creates (val, id) tuple
void create_tuple(ClusterTuple* cmp, int* data, int num_elts) {
	for (int i = 0; i != num_elts; i++) {
		cmp[i].val = data[i];
		cmp[i].id = i;
	}
}

int max(int* data, int num_vals) {
	int cur = INT_MIN;

	for (int i = 0; i != num_vals; i++) {
		if (data[i] > cur) {
			cur = data[i];
		}
	}
	return cur;

}

// binary search that takes in clusterTuple
int binary_search2(ClusterTuple* search_array, int target, int len) {
	int start = 0; 
    int result = -1; 
    while (start <= len) { 
        int mid = (start + len) / 2; 
        // Move to right side if target is 
        // greater. 
        if (search_array[mid].val <= target) {
            start = mid + 1; 
        } else { 
            result = mid; 
            len = mid - 1; 
        } 
    }

 
    // make sure to find the first instance (value might be repeated several times)
    if (result >= 0) {
	    while (search_array[result].val >= target ) {
	    	result--;
	    }

	    result++;
	}

  
    return result; 
}


// binary search for btree search
int binary_search(int* search_array, int target, int len) { 
    int start = 0; 
    int result = -1; 
    while (start <= len) { 
        int mid = (start + len) / 2; 
        // Move to right side if target is 
        // greater. 
        if (search_array[mid] <= target) {
            start = mid + 1; 
        } else { 
            result = mid; 
            len = mid - 1; 
        } 
    }
 
    // takes care of repeated values
    if (result >= 0) {
	    while (search_array[result] >= target ) {
	    	result--;
	    }

	    result++;
	}

  
    return result; 
} 


// comparator for unclustered index qsort
int comparator_uncl(const void *p, const void *q) {
	int val1 = *(const int*) p;
	int val2 = *(const int*) q;

	if (val1 == val2) {
		return 0;
	} else if (val1 > val2) {
		return 1;
	} else {
		return -1;
	}
}

// comparator for clustered index qsort call
int comparator_clust(const void *p, const void *q) {
	const ClusterTuple* cl1 = (const ClusterTuple*) p;
	const ClusterTuple* cl2 = (const ClusterTuple*) q;

	if (cl1->val == cl2->val) {
		return 0;
	} else if (cl1->val > cl2->val) {
		return 1;
	} else {
		return -1;
	}
}
	
// loads the btree leafs (cache-conscious)
// inputs: column and number of items to insert
// returns: head BtreeNode pointer.
BtreeNode* load_btree_base(Column* col, int num_elts) {
	ClusterTuple* ct = NULL;

	// if not clustered index then I want to load (val, id) in the leafs
	if (!col->clustered) {
		ct = malloc(sizeof(ClusterTuple)*num_elts);
		create_tuple(ct, col->data, num_elts);	
		qsort((void*) ct, num_elts,sizeof(col->index->sorted[0]), comparator_uncl);
	}
	int num_cpy = num_elts;
	
	int* tmp_col = malloc(sizeof(int)*num_elts);
	if (!tmp_col) {
		return NULL;
	}

	// copy and sort the data
	memcpy(tmp_col, col->data, num_elts*4);
	qsort((void*) tmp_col, num_elts, sizeof(int), comparator_uncl);
	int num_nodes_ini = (num_elts % (PAGESIZE/4) == 0) ? 0:1;
	int num_nodes = num_elts/(PAGESIZE/4) + num_nodes_ini;

	// allocate space for the leaves
	BtreeNode* leafs = malloc(sizeof(BtreeNode) * num_nodes);

	if (!leafs) {
		return NULL;
	}

	int per_page = 0;
	for (int i = 0; i != num_nodes; i++) {
 
 		// keep track of the last node in the leaf blocks
		if (i == num_nodes-1) {
				leafs[i].last_node = true;
			} else {
				leafs[i].last_node = false;
			}
		int temp_val = (num_cpy > (PAGESIZE/4)) ? (PAGESIZE/4):num_cpy;
	 
	 	// place values in leafs
		memcpy(leafs[i].data, tmp_col + per_page, temp_val*4);
		
		// store ids for unclustered btree
		if (!col->clustered) {
			for (int k = 0; k != temp_val; k++) {
				leafs[i].orig_ids[k] = ct[per_page+k].id;
			}
		 }
		per_page += temp_val;
 
		num_cpy -= temp_val;
		leafs[i].child_block = NULL;
		leafs[i].leaf = true;

		leafs[i].num_elt = temp_val;
 
		leafs[i].start_id = i*(PAGESIZE/4);
  
	}

 
	return leafs;
}

// load parent nodes
// input: leafs and number of leafs
// returns: root BtreeNode
// this process is similar to loading the leafs
BtreeNode* load_btree_internal(BtreeNode* lfs, int num_elts) {
	int num_nodes = 2;
	BtreeNode* internals = NULL;
	while (num_nodes > 1) {
		int num_cpy = num_elts;
		
		 
		num_nodes = num_elts/(PAGESIZE/4);
		if (num_nodes == 0) {
			num_nodes = 1;
		}
		internals = malloc(sizeof(BtreeNode) * num_nodes);
		int leaf_index = 0;
	 
		for (int j = 0; j != num_nodes; j++) {
			if (j == num_nodes-1) {
				internals[j].last_node = true;
			} else {
				internals[j].last_node = false;
			}
			int temp_val = (num_cpy > (PAGESIZE/4)) ? (PAGESIZE/4):num_cpy;
			internals[j].child_block = lfs + leaf_index;
			for (int i = 0; i != temp_val; i++) {
				internals[j].data[i] = max(lfs[leaf_index].data, lfs[leaf_index].num_elt);
				 
				leaf_index++;
			}
			
			internals[j].leaf=false;
			internals[j].num_elt = temp_val;

			num_cpy -= (temp_val+1);

		}

		lfs = internals;
		num_elts = num_nodes;
	}



	return internals;
}




// unclustered index with array
void create_unclustered_array(Column* col, int num_elts) {
	
	col->index = malloc(sizeof(ColumnIndex));

	if (!col->index) {
		printf("MALLOC FAILED\n");
	}
	col->index->sorted = malloc(sizeof(ClusterTuple) * num_elts);
	
	if (!col->index->sorted) {
		printf("MALLOC FAILED\n");
	}

	// create (val, id) tuple
	create_tuple(col->index->sorted, col->data, num_elts);	
	qsort((void*) col->index->sorted, num_elts,sizeof(col->index->sorted[0]), comparator_uncl);

}

// unclustered index with btree
void create_unclustered_btree(Column* col, int num_elts) {

		// allocate space for index
		col->index = malloc(sizeof(ColumnIndex));
		col->index->data_tree = malloc(sizeof(BtreeNode));

		if (!col->index || !col->index->data_tree) {
			printf("MALLOC FAILED\n");
		}

		int num_nodes_ini = (num_elts % (PAGESIZE/4) == 0) ? 0:1;
		int num_nodes = num_elts/(PAGESIZE/4) + num_nodes_ini;
		BtreeNode* base = NULL;
		BtreeNode* root = NULL;
		
		// load btree
		base = load_btree_base(col, num_elts);
		root = load_btree_internal(base, num_nodes);
		col->index->data_tree = root;	
}


// create clustered index with array
void create_clustered_array(Column* col, Table* tbl, int num_elts) {
	ClusterTuple* cmp_array = malloc(sizeof(ClusterTuple)*num_elts);
	if (!cmp_array) {
		printf("MALLOC FAILED\n");
	}

	create_tuple(cmp_array, col->data, num_elts);
	qsort((void*) cmp_array, num_elts, sizeof(cmp_array[0]), comparator_clust);
	// create copy of table
	tbl->cluster_copy = malloc(sizeof(Column)*tbl->col_count); 

	// create copy of the base data while propogating order of column
	for (int j = 0; j != (int)tbl->table_length; j++) {
		for (int i = 0; i != (int)tbl->col_count; i++) {
			if (j == 0) {
				tbl->cluster_copy[i].data = malloc(sizeof(int) * tbl->table_length);
				tbl->cluster_copy[i].type = SORTED;
				tbl->cluster_copy[i].index = NULL;
				tbl->cluster_copy[i].clustered = false;
				strcpy(tbl->cluster_copy[i].name, tbl->columns[i].name);
			}
			
			tbl->cluster_copy[i].data[j] = tbl->columns[i].data[cmp_array[j].id];
		}
	}
	col->index = malloc(sizeof(ColumnIndex));
	
	if (!col->index) {
		printf("MALLOC FAILED!\n");
	}

	// create clustered btree index
	if (col->type == BTREE) {
		//printf("line 210 btree.c\n");
		col->index = malloc(sizeof(ColumnIndex));
		col->index->data_tree = malloc(sizeof(BtreeNode));
		int num_nodes_ini = (num_elts % (PAGESIZE/4) == 0) ? 0:1;
		int num_nodes = num_elts/(PAGESIZE/4) + num_nodes_ini;
		BtreeNode* base = NULL;
		BtreeNode* root = NULL;
		//printf("line 217 btree.c\n");
		base = load_btree_base(col, num_elts);
		//printf("line 219 btree.c\n");
		root = load_btree_internal(base, num_nodes);
		col->index->data_tree = root;			
	} else {
		col->index->sorted = cmp_array;
	}

}


// search btree to fulfill range query
int btree_search(BtreeNode* root, int target) {
	BtreeNode* tmp = root;
	if (!tmp) {
		return -1;
	}
 
	int num_layers = 0;

	// count number of levels in btree
	while (tmp) {
 
		tmp = tmp->child_block;
 
		num_layers++;
	}
	tmp = root;
	
	int return_val = -1;
	
	// start search
	while (tmp) {
 		// if you found the correct leaf with the desired value
		if (num_layers==1) {
 
			return_val = binary_search(tmp->data, target, tmp->num_elt);
			if (return_val >= 0) {
			 	return_val += tmp->start_id;
			} 
			
			return return_val;
		} else {

			// traverse the parent nodes to find the correct leaf node
			for (int i = 0; i < tmp->num_elt; i++) {
				if (tmp->data[i] >= target) {
					
					tmp = &(tmp->child_block[i]);
					num_layers--;
					break;
				} else if ((tmp->data[i] < target) && (i == tmp->num_elt-1)) {
					if (tmp->child_block[i].last_node) {
						
						tmp = &(tmp->child_block[i]);
					} else {
						
						tmp = &(tmp->child_block[i+1]);
					}
					num_layers--;
					break;
				}
			}
		}
	}

	return return_val;
}

// function is initially called in server to create the index
void handle_index(Column* col, Table* tbl, StoreType type, bool is_clustered) {
	int loc_flg = 0;

	if (!col->index) {
		loc_flg = 1;
	}
	
	col->type = type;
	col->clustered = is_clustered;
	
	// check what type of index is being created
	if (type == SORTED && is_clustered) {
	  create_clustered_array(col,tbl, tbl->table_length);
		
	} else if (type == SORTED && !is_clustered) {
		create_unclustered_array(col, tbl->table_length);

	} 
	else if (type == BTREE && is_clustered) {

		create_clustered_array(col, tbl, tbl->table_length);
	} else if (type == BTREE && !is_clustered) {
		create_unclustered_btree(col, tbl->table_length);
	}

	if (loc_flg == 1) {
		tbl->num_idx++;
	}



}



// select function that stores the start and end indices
void clustered_select(Result* res, int index_needed, Table* table, Column* column, char* interm, int lower, int upper) {


	int lookup = find_result(res, interm, index_needed);
	if (lookup > -1) {
		index_needed = lookup;
	}

	Column* col = NULL;

	// find column in copy of the data
	for (int i = 0; i != (int)table->col_count; i++) {
 
	  if (strcmp(column->name, table->cluster_copy[i].name) == 0) {
	    col = &table->cluster_copy[i];
 
	    break;
	  }
	}


	

	int start_ind = 0;
	int end_ind = 0;

	// if index is an array we can simply use binary search 
	// otherwise traverse the btree
	if (column->type == SORTED) {
		start_ind = binary_search(col->data, lower, table->table_length);
		end_ind = binary_search(col->data, upper, table->table_length);
	} else {
		start_ind = btree_search(column->index->data_tree, lower);
  
		end_ind = btree_search(column->index->data_tree, upper);
 
	}
 		// store the start and end indices (data is sorted)
	    res[index_needed].payload[0] = start_ind;
	    res[index_needed].payload[1] = end_ind;
	   
 
	    res[index_needed].num_tuples=2;
	    res[index_needed].select_type = RANGE;
	

	res[index_needed].nm[0] = '\0';
        strcpy(res[index_needed].nm, interm);

	res[index_needed].data_type=INT;
	res[index_needed].orig_lngth = table->table_length;
	


	// my issue is in relational insert 
	// where I assign the data and hash the column
	// table hash seems to be working though

}


// since I stored indices for an unclustered index
// I can find the positions needed for range queries
void unclust_select(Result* res, int index_needed, Table* table, Column* column, char* interm, int lower, int upper) {
	int lookup = find_result(res, interm, index_needed);
	if (lookup > -1) {
		index_needed = lookup;
	}



	// check if array index or btree index
	if (column->type == SORTED) {

		int start_ind = binary_search2(column->index->sorted, lower, table->table_length);
		int end_ind = binary_search2(column->index->sorted, upper, table->table_length);

		int j = 0;

		for (int i = start_ind; i != end_ind; i++) {
			res[index_needed].payload[j] = column->index->sorted[i].id;
			j++;
		}
		res[index_needed].num_tuples = j;
	} else {
		
 
		int start_ind = btree_search(column->index->data_tree, lower);
 
		if (start_ind >= 0 || lower < column->data[0]){
			if (start_ind == -1) {
				start_ind = 0;
			}
 
			BtreeNode* leafs = column->index->data_tree;
			while (leafs->child_block) {
		//		printf("num_iter line 537\n");
				leafs = leafs->child_block;
			}
 
			int j = start_ind/(PAGESIZE/4);
			int keep_track_of_sz = start_ind;
			int count = (start_ind % (PAGESIZE/4));
			int payload_start = 0;
			while (leafs[j].data[count%(PAGESIZE/4)] < upper && keep_track_of_sz < (int)table->table_length) {
				res[index_needed].payload[payload_start]=leafs[j].orig_ids[count%(PAGESIZE/4)];
				 
				count++;
				payload_start++;
				keep_track_of_sz++;
				j += ((count%(PAGESIZE/4) == 0) ? 1:0); 
			}
			res[index_needed].num_tuples = payload_start;
		} else {
			res[index_needed].num_tuples = 0;
		}
	}
	
	// store id's in result array
	res[index_needed].orig_lngth = table->table_length;
	res[index_needed].data_type = INT;
	res[index_needed].select_type = IDX;
	strcpy(res[index_needed].nm, interm);

}


// fetch to be called after cluster_select
void cluster_fetch(Result* res, int ind_needed, Table* table, Column* column, char* interm, Result* intermediate) {
	if (!column) {
		printf("ERROR\n");
	}
	

	int lookup = find_result(res, interm, ind_needed);
	if (lookup > -1) {
		ind_needed = lookup;
	}

	Column* col = NULL;
	
	for (int i = 0; i != (int)table->col_count; i++) {

	  if (strcmp(column->name, table->cluster_copy[i].name) == 0) {
	    col = &table->cluster_copy[i];
	  }
	}

	int num_stored = 0;

	if (intermediate->select_type == RANGE) {
	  num_stored = intermediate->payload[1] - intermediate->payload[0];

	  memcpy(res[ind_needed].payload, col->data + intermediate->payload[0], sizeof(int)*num_stored);

	}

	strcpy(res[ind_needed].nm, interm);
	res[ind_needed].num_tuples = num_stored;
	res[ind_needed].data_type = INT;
	res[ind_needed].select_type = FTH;

}


// fetch function for select query stored as an id array 
void id_fetch(Result* res, int ind_needed, Column* column, Table* table, char* interm, Result* intermediate) {
//	printf("enter id_fetch: %i\n", intermediate->num_tuples);
	int lookup = find_result(res, interm, ind_needed);
	if (lookup > -1) {
		ind_needed = lookup;
	}

	if (intermediate->select_type == IDX) {
		for (int i = 0; i != (int)intermediate->num_tuples; i++) {
			res[ind_needed].payload[i] = column->data[intermediate->payload[i]];
		}
	} else {
		Column* col = NULL;
	 	// get appropriate ID's
		for (int i = 0; i != (int)table->col_count; i++) {
 
		  if (strcmp(column->name, table->cluster_copy[i].name) == 0) {
	 
		    col = &table->cluster_copy[i];
		    break;
		  }
		}
		for (int i = 0; i != (int)intermediate->num_tuples; i++) {
			res[ind_needed].payload[i] = col->data[intermediate->payload[i]];
		}
	}

	strcpy(res[ind_needed].nm, interm);
	res[ind_needed].num_tuples = intermediate->num_tuples;
	res[ind_needed].data_type = INT;
	res[ind_needed].select_type = FTH;
}


/// JOIN FXNS ///

void _join(char* store1, char* store2, Result* fetch1, Result* fetch2, Result* sel1,
	 Result* sel2, char* method, Result* chand_tbl, int num_elt) {
	int index1 = num_elt;
	int index2 = num_elt + 1;
	int lookup1 = find_result(chand_tbl, store1, num_elt);
	if (lookup1 > -1) {
		index1 = lookup1;
	}

	int lookup2 = find_result(chand_tbl, store2, num_elt);
	if (lookup2 > -1) {
		index2 = lookup2;
	}

	int counter = 0;
	int k = 0;
	int l = 0;

	// convert bitvector into id array
	int* buf1 = malloc(sizeof(int) * fetch1->num_tuples);
	int* buf2 = malloc(sizeof(int) * fetch2->num_tuples);

	memset((void*)buf1, 0, 4*fetch1->num_tuples);
	memset((void*)buf2, 0, 4*fetch2->num_tuples);

	for (int i = 0; i != sel1->orig_lngth; i++) {
	 
		if (sel1->payload[i] == 1) {
			buf1[k] = i;
			k++;
		}
	}

	for (int i = 0; i != sel2->orig_lngth; i++) {
		if (sel2->payload[i] == 1) {
			buf2[l] = i;
			l++;
			
		}
	}
		// check if nested loop join or hash join
		if (strncmp(method, "nested", 6) == 0) {

			for (int i = 0; i != (int)fetch1->num_tuples; i++) {
				for (int j = 0; j != (int)fetch2->num_tuples; j++) {
					if (fetch1->payload[i] == fetch2->payload[j]) {

						chand_tbl[index1].payload[counter] = buf1[i];

						chand_tbl[index2].payload[counter] = buf2[j];
						
						counter++;
					}
				}
			}
		} else {
		hashtable* ht =  NULL;
		
		// allocate hashtable
		int fail = allocate(&ht, fetch1->num_tuples);

		if (fail==-1) {
			printf("FAILURE TO ALLOCATE HASHTABLE\n");
		}

		// build hash table on smaller column
		for (int i = 0; i != (int)fetch1->num_tuples; i++) {
			put(ht, (keyType)fetch1->payload[i], (valType) buf1[i]);
		}
		int num_res = 0;

		for (int j = 0; j != (int)fetch2->num_tuples; j++) {
			int tmppp = hash(fetch2->payload[j]);

			int num_to_retrieve = ht->num_res[tmppp];
			
			// check that there are matching values
			if (num_to_retrieve > 0) {		

				int* ress = malloc(sizeof(int)*num_to_retrieve);
				if (!ress) {
					printf("Error in Allocation\n");
				}
				int fail = get(ht, fetch2->payload[j], ress, num_to_retrieve, &num_res);
				
				// check that hash table look up did not fail
				if (fail != -1) {
					//printf("found!");
					int inner_cnt = 0;
					while (num_res > 0) {
						chand_tbl[index1].payload[counter] = ress[inner_cnt];
						chand_tbl[index2].payload[counter] = buf2[j];
						inner_cnt++;
						num_res--;
						counter++;
					} 
					
				}
			}
			
		}

	}

 
	strcpy(chand_tbl[index1].nm, store1);
	strcpy(chand_tbl[index2].nm, store2);

	chand_tbl[index1].data_type = INT;
	chand_tbl[index1].select_type = IDX;
	chand_tbl[index1].num_tuples =counter;

	chand_tbl[index2].data_type = INT;
	chand_tbl[index2].select_type = IDX;
	chand_tbl[index2].num_tuples = counter;

}





