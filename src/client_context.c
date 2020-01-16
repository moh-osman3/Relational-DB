#include "client_context.h"
#include "cs165_api.h"
#include <string.h>
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */

// Table* table_lookup[1001];

int glob_mes_to_receive = 0;

// unsigned int hash(unsigned char *str) {
//         unsigned int hash = 5381;
//         int c;

//         while (c = *str++) {
//             hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
//         }

//         return hash % 2003;
//     }

// Table* lookup_table(char *name, bool flag) {
// 	// void pattern for 'using' a variable to prevent compiler unused variable warning
	
// 	if (!flag) {

// 		unsigned int index = hash((unsigned char*) name);

// 		if (!table_lookup[index]) {
// 			return NULL;
// 		}
// 		return table_lookup[index];

// 	}


// 	int counter = 0;

// 	while (name[counter] != '.') {
// 		counter++;
// 	}
// 	char tabl_name[strlen(name) - counter-2];
//     strncpy(tabl_name, name + counter+1, strlen(name) - counter-1);
//     tabl_name[strlen(name)] = '\0';
	
// 	printf("%s\n", tabl_name);


// 	unsigned int index = hash((unsigned char*) tabl_name);

// 	if (!table_lookup[index]) {
// 		return NULL;
// 	}
// 	return table_lookup[index];
// }

// Column* find_column(char* name) {
// 	unsigned int index = hash((unsigned char*) name);

// 	if (!(column_lookup[index])) {
// 		return NULL;
// 	}

// 	return column_lookup[index];

// }

int lookup_table(char* name) {
	int length = (1 > current_db->tables_size) ? 1 : current_db->tables_size;
	printf("length in table lookup: %i\n", current_db->tables_size);
	for (int i = 0; i != length; ++i) {
		if (current_db) {
			if ((int) current_db->tables_size <= i) {
				return -1;
			}
		} else {
			return -1;
		}
		Table current_table = current_db->tables[i];
		printf("current: %s\n", current_table.name);
		printf("tbl to compare: %s\n", name);
		if (strcmp(current_table.name, name) == 0) {
			// printf("SUCCESS IN TABLE LOOKUP!\n");
			// printf("lookup table table name: %zu\n", current_table.table_length);
			return i;
		} 
	}
	printf("TABLE NOT FOUND\n");
	return -1;
}

int find_column(char* table_name, char* col_name) {
	int ind_t = lookup_table(table_name);
	if (ind_t == -1) {
		return -1;
	}
	Table* table_want = &current_db->tables[ind_t];
	printf("error1\n");
	int length = table_want->col_count;
	printf("error2\n");
	for (int i = 0; i != length; ++i) {
		//printf("error3\n");
		
		Column current_col = table_want->columns[i];
		// printf("col_name find col : %s\n", current_col.name);
		// printf("column_name to compare to: %s\n", col_name);
		if (strcmp(current_col.name, col_name) == 0) {
			//printf("SUCCESS IN COLUMN LOOKUP\n");
			Column* col = malloc(sizeof(Column));
			if (!col) {
				//printf("WHAT THE HECK\n");
			}
		//	printf("just checking\n");
			*col = table_want->columns[i];
			strcpy(col->name, current_col.name);

			//printf("one more\n");
			return i;
		}
	}

	printf("COLUMN NOT FOUND\n");
	return -1;
}



int find_result(Result* result_table, char* result_name, int num_to_iter) {
	     int needed_result_index = -1;
	     printf("mmmmm: %s\n", result_name);
	     printf("line 124 client_context.c: %i \n", num_to_iter);
        //printf("%i\n", result_lookup[0]->num_tuples);
	     //printf("num to iter: %i \n", nu)
        for (int i = 0; i != num_to_iter; ++i) {
           // printf("help5\n");

            char* comp_name = result_table[i].nm;
           // printf("nnnnnn : %s\n",result_table[i].nm);
            if (strcmp(comp_name, result_name) == 0) {
              //  printf("help4\n");
                needed_result_index = i;
                return i;
                break;
            }
        }
        printf("Line 137 client_context.c\n");

        if (needed_result_index == -1) {
        	printf("RESULT NOT FOUND\n");
        	return -1;
        } else {
        	printf("Result found\n");
        	return needed_result_index;
        }
}





/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/
