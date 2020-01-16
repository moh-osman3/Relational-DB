#include "client_context.h"
#include "cs165_api.h"
#include <string.h>


int glob_mes_to_receive = 0;


// keeps track of tables 
int lookup_table(char* name) {
	int length = (1 > current_db->tables_size) ? 1 : current_db->tables_size;
 
	for (int i = 0; i != length; ++i) {
		if (current_db) {
			if ((int) current_db->tables_size <= i) {
				return -1;
			}
		} else {
			return -1;
		}

		Table current_table = current_db->tables[i];
		
		// iterate through and compare names til you find desired table
		if (strcmp(current_table.name, name) == 0) {
			return i;
		} 
	}
	printf("TABLE NOT FOUND\n");
	return -1;
}

// lookup for columns
int find_column(char* table_name, char* col_name) {
	int ind_t = lookup_table(table_name);
	if (ind_t == -1) {
		return -1;
	}
	Table* table_want = &current_db->tables[ind_t];
 
	int length = table_want->col_count;
 
	for (int i = 0; i != length; ++i) {
	 
		
		Column current_col = table_want->columns[i];
	 
		if (strcmp(current_col.name, col_name) == 0) {

			Column* col = malloc(sizeof(Column));
			if (!col) {
				//printf("WHAT THE HECK\n");
			}
 
			*col = table_want->columns[i];
			strcpy(col->name, current_col.name);

	 
			return i;
		}
	}

	printf("COLUMN NOT FOUND\n");
	return -1;
}


// lookup for results (this is searches the variable pool result_table)
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


