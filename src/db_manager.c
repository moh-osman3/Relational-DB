#define _DEFAULT_SOURCE
#include "cs165_api.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "client_context.h"
#include "utils.h"
#include "parse.h"
 
#include <pthread.h>
 
#define THREADS 2
// In this class, there will always be only one active database at a time
Db *current_db=NULL;

int global_counter1 = 0;
int global_table_length = 0;

// Function which performs AND operation for lazy deletes
void array_AND(int* store, int* comp, int num_vals) {
	for (int i = 0; i != num_vals; i++) {
		store[i] |= comp[i];
	}
}

void delete_update(Table* tbl, Result* res) {
	 	//printf("line 23 db.c: %i\n", tbl->table_length);
		array_AND(tbl->chandle_deletes->payload, res->payload, tbl->table_length);

 
}
 
// function to take care of updates 
void update(Column* column, Table* table, Result* result, int value) {
	
 	// check type of select result
	if (result->select_type == IDX) {
		 
		for (int i = 0; i != result->num_tuples; i++) {
			column->data[result->payload[i]] = value;
		}
	} else if (result->select_type == BIT) {
	 
		for (int i = 0; i != result->orig_lngth; i++) {
			if (result->payload[i] == 1) {
				column->data[i] = value;
			}
		}
	} else if (result->select_type == RANGE) {
 
		Column col_for_sel;
		for (int i = 0; i != table->col_count; i++) {
			if (table->columns[i].clustered) {
				col_for_sel = table->columns[i];
			}
		}
		for (int i = result->payload[0]; i < result->payload[1]; i++) {
			int id = col_for_sel.index->sorted[i].id;
			column->data[id] = value;
		}
	}
}

/* 
 * Arithmetic helper functions
 */
long long _sum(Result* result) {
	long long count = 0;

	for (int i = 0; i != ((int)result->num_tuples); ++i) {
		count += ((long long) result->payload[i]);
 
	}

	return count;
}

int _max(Result* result, int* res) {
	int cur_max = result->payload[0];
	for (int i = 1; i != ((int)result->num_tuples); ++i) {
 
		if (result->payload[i] > cur_max) {
			cur_max = result->payload[i];
		}
	}
	*res = cur_max;
	return cur_max;
}

int _min(Result* result) {
	int cur_min = result->payload[0];
	for (int i = 1; i != ((int)result->num_tuples); ++i) {
		if (result->payload[i] < cur_min) {
			cur_min = result->payload[i];
		}
	}
	return cur_min;
}

 double _avg(Result* result) {
	long long total =  _sum(result);
	if (result->num_tuples == 0) {
		return 0.0;
	}
 
	long double avgg = ((long double)(total))/((long double) result->num_tuples);
 
	return avgg;
}

// main fucnction that does add and subtract operations
void do_binop(int ind_needed, char* operation, char* store_as, Result* res1, Result* res2, Result* result_list) {
	int already_here = find_result(result_list, store_as, ind_needed);

	if (already_here > -1) {
		ind_needed = already_here;
	}

	strcpy(result_list[ind_needed].nm, store_as);

	if (strncmp(operation, "add", 3) == 0) {
		for (int i = 0; i != ((int) res1->num_tuples); ++i) {
			result_list[ind_needed].payload[i] = res1->payload[i] + res2->payload[i];	
			//printf("Line 66 db_m.c result of add: %i + %i = %i\n", res1->payload[i], res2->payload[i],result_list[ind_needed].payload[i]);		
		}
	} else {
		for (int i = 0; i != ((int) res1->num_tuples); ++i) {
			result_list[ind_needed].payload[i] = res1->payload[i] - res2->payload[i];
			//printf("Line 71 db_m.c result of add: %i\n", result_list[ind_needed].payload[i]);			
		}
	}
	result_list[ind_needed].num_tuples = res1->num_tuples;
	result_list[ind_needed].data_type = INT;
	result_list[ind_needed].select_type = OP;
}

// main function that performs sum,min,max,avg operators
void do_arith(int ind_needed, char* operation, char* store_as , Result* column_for_op, Result* result_list) {
 
	int result_of_arith = 0;
	long long result_sum;
	int already_here = find_result(result_list, store_as, ind_needed);

 
	if (already_here > -1) {
		ind_needed = already_here;
	}

 
	long double result_avg;
	result_list[ind_needed].num_tuples = 1;
	strcpy(result_list[ind_needed].nm, store_as);
	int flag = 0;
	if (strncmp(operation, "avg", 3) == 0) {
		flag=1;
		result_avg = _avg(column_for_op);
 
		result_list[ind_needed].payload_float[0] = result_avg;
		result_list[ind_needed].data_type = FLOAT;
		result_list[ind_needed].select_type = OP;
	}
	else if (strncmp(operation, "sum", 3)==0) {
		flag=2;
		result_sum = _sum(column_for_op);
 
		
	} else if (strncmp(operation, "min", 3)==0) {

		result_of_arith = _min(column_for_op);
 
	} else if (strncmp(operation, "max", 3)==0) {
		_max(column_for_op,&result_of_arith);
	 
	}

	if (flag == 0) {
 
		result_list[ind_needed].payload[0] = result_of_arith;
		result_list[ind_needed].data_type = INT;
		result_list[ind_needed].select_type = OP;
	} else if (flag == 2) {
		 
		result_list[ind_needed].payload_long[0] = result_sum;

		result_list[ind_needed].data_type = LONG;
		result_list[ind_needed].select_type = OP;
	}

	

}

void fetch(Result* res, int ind_needed, Table* table, Column* column, char* interm, Result* intermediate) {
	if (!column) {
		printf("error\n");
	}

	int lookup = find_result(res, interm, ind_needed);
	if (lookup > -1) {
		ind_needed = lookup;
	} 
	int num_stored = 0;

	// Iterate through and store result if bitvector value is 1
	for (int i = 0; i != ((int) table->table_length); ++i) {
		// also make sure to check that value has not been deleted
		if (intermediate->payload[i] == 1 && table->chandle_deletes->payload[i] == 0) {
			res[ind_needed].payload[num_stored] = column->data[i];
			num_stored++;
		}
	}
	res[ind_needed].num_tuples = num_stored;
	res[ind_needed].data_type = INT;
	res[ind_needed].select_type = FTH;
	strcpy(res[ind_needed].nm, interm);
	
}

// int _printt(Result* result, int iterate_til, char* name_of_print) {
// 	for (int i = 0; i < iterate_til; ++i) {
// 		//printf("name of result in print: %s\n", result[i].nm);
// 		if (strcmp(result[i].nm, name_of_print) == 0) {
// 			//printf("number of results in print: %zu \n", result[i].num_tuples);
// 			for (int j = 0; j < ((int) result[i].num_tuples); ++j) {
// 				printf("%i\n", result[i].payload[j]);
// 			} 
		
// 			return i;
// 		}
// 	}
// 	return -1;

// }


// select function which handles a select on another select query
void _select2(Result* res_bitvector, Result* res_for_range, Result* res, 
				int num_results, char* name, int lower, int upper)
{
	int lookup = find_result(res, name, num_results);
	if (lookup > -1) {
		num_results = lookup;
	}

	int number_of_qual = 0;
	int num_ones = 0;
	if (res_bitvector->select_type == BIT) {
		for (int i = 0; i != res_bitvector->orig_lngth; ++i) {
	
			
					int tmp1 = (res_bitvector->payload[i] == 1) ? 1:0;
					int tmp2 = (res_for_range->payload[num_ones] >= lower && res_for_range->payload[num_ones] < upper ) ? 1:0;
					
						res[num_results].payload[i] = (tmp2*tmp1);
						number_of_qual+=tmp2;

				num_ones+=tmp1;
		}
		res[num_results].select_type = BIT;
	} else {
		for (int i = 0; i < (int)res_for_range->num_tuples; i++) {
			if (res_for_range->payload[i] >= lower && res_for_range->payload[i] < upper) {
				res[num_results].payload[number_of_qual] = res_bitvector->payload[0] + i;
				number_of_qual++;
			}

			res[num_results].select_type = IDX2;

		}
	}
		res[num_results].num_tuples=number_of_qual;

		

		res[num_results].nm[0] = '\0';
		strcpy(res[num_results].nm, name);
		
		res[num_results].data_type=INT; 
		
	}

// regular select function that stores its result as a bitvector
Result* selectt(Result* res, int index_needed, Table* table, Column* column, char* interm, int lower, int upper) {

	int lookup = find_result(res, interm, index_needed);
	// Check if result name already exists
	if (lookup > -1) {
		index_needed = lookup;
	} 
	
	int num_ones = 0;
	for (int i = 0; i != ((int) table->table_length); ++i) {
		// check whether value qualifies
		int tmpp = (column->data[i] >= lower && column->data[i] < upper) ? 1:0;
		res[index_needed].payload[i] = tmpp;
		num_ones+=tmpp;
			
	}

	res[index_needed].num_tuples=num_ones;

	res[index_needed].nm[0] = '\0';
	strcpy(res[index_needed].nm, interm);
		

	res[index_needed].data_type=INT;
	res[index_needed].select_type = BIT;
	res[index_needed].orig_lngth = table->table_length;
	
	
	
	return res; 

	// my issue is in relational insert 
	// where I assign the data and hash the column
	// table hash seems to be working though

}

Status relational_insert(Table* table, int* values) {
	size_t num_cols = table->col_count;
	Status ret_status;


	size_t num_vals = table->table_length;

	// check that there is space allocated 
	if (table->table_length == 0) {
		for (int i = 0; i != ((int) num_cols); ++i) {
						
			table->columns[i].data = (int*) malloc(1000*sizeof(int));
			
			if (!table->columns[i].data) {

			}
		}	
	}

		
	for (int i = 0; i != ((int)num_cols); ++i) {
			table->columns[i].data[num_vals] = values[i];
	}

	table->table_length++;
	global_table_length++;

	ret_status.code = OK;

	return ret_status;


}


// function that concatenates db.tbl.col
char* full_name(char*db, char*tbl,char*col) {
	int db_n = strlen(db);
	int tbl_n = strlen(tbl);
	int col_n = strlen(col);

	char* ret_name = malloc(db_n+tbl_n+col_n+3);

	strcpy(ret_name, db);
	strcpy(ret_name+db_n, ".");
	strcpy(ret_name+db_n+1, tbl);
	strcpy(ret_name+db_n+tbl_n+1, ".");
	strcpy(ret_name+db_n+tbl_n+2, col);
	return ret_name;

}

Status create_column(char* name, Table* table) {
	Status ret_status;
	
	// Make sure column name is not too long
	if (strlen(name) > MAX_SIZE_NAME) {
		ret_status.code = ERROR;
		ret_status.error_message = "Name too long";
		return ret_status;
	}

	if (!table) {
		printf("error\n");
	}

	
	strcpy(table->columns[global_counter1].name, trim_quotes(name));
	
	table->columns[global_counter1].data = (int*) malloc(sizeof(int)*10000000);
	table->columns[global_counter1].index = NULL;
	table->columns[global_counter1].type = NOIDX;
	table->columns[global_counter1].num_in_pool = 0;
	//table->counter++;
	global_counter1++;
	
	// hash full column name
	
	ret_status.code=OK;
	table->counter++;
	return ret_status;
}

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning

	// check that table name isn't too long
	if (strlen(name) > MAX_SIZE_NAME) {
		ret_status->code = ERROR;
		ret_status->error_message = "Name too long";
		return NULL;
	}
	global_table_length=0;
	global_counter1 = 0;
	// create table and columns
	Table* table = malloc(sizeof(Table));

	int index = lookup_table((char*)name);
	bool increment = false;
	if (index == -1) {
		index = db->tables_size;
		increment = true;
	}
	strcpy(current_db->tables[index].name, name);
	current_db->tables[index].columns = (Column*) malloc(sizeof(Column)*50000);
	current_db->tables[index].counter = 0;
	current_db->tables[index].col_count = num_columns;
	current_db->tables[index].table_length = 0;
	current_db->tables[index].cluster_copy = NULL;
	current_db->tables[index].num_idx = 0;
	current_db->tables[index].chandle_deletes = NULL;
	current_db->tables[index].col_names_to_index = malloc(10*sizeof(char*));
	current_db->tables[index].index_create = 0;
	current_db->tables[index].chandle_deletes = malloc(sizeof(Result));
	for (int i = 0; i != SIZEOFRES; i++) {
		current_db->tables[index].chandle_deletes->payload[i] = 0;
	}

	if (increment) {
		current_db->tables_size++;
	}
	ret_status->code=OK;

	
	return table;
}

/* 
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char* db_name) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	
	if (strlen(db_name) > MAX_SIZE_NAME) {
		Status ret_stat = {ERROR, "Name too long"};
		return ret_stat;
	}


	current_db = (Db*) malloc(sizeof(Db));

	strcpy(current_db->name, db_name);
	current_db->tables = malloc(sizeof(Table)*100);
	current_db->tables_size = 0;
	current_db->tables_capacity = 0;
	current_db->query_batch = malloc(sizeof(char*)*500);
	current_db->clust_exists = false;
	current_db->unclust_exists = false;
	current_db->wt = 0;
	current_db->off = false;
	current_db->partial_shutdown = false;

	for (int i = 0; i != 500; i++) {
		current_db->query_batch[i] = NULL;

	}
	current_db->num_in_batch = 0;

	


	(void) (db_name);
	struct Status ret_status;
	
	ret_status.code = OK;
	return ret_status;
}

// function that places commas and outputs a line to write

char* place_commas(char** string_list, int num_elts, int size_of_output) {
    

    char out_buf[10000];

    int index = 0;
    for (int i = 0; i != num_elts-1; ++i) {
    	//printf("The first thing: %s\n", string_list[i]);
        strcpy(out_buf+index, string_list[i]);
        index += strlen(string_list[i]);
        strcpy(out_buf+index, ",");
        index+=1;
    }
    strcpy(out_buf+index, string_list[num_elts-1]);
    out_buf[size_of_output+num_elts-1] = '\0';
    char* ret_buf = malloc(size_of_output+num_elts);
    strcpy(ret_buf, out_buf);
    return ret_buf; 


}

// create the name of a data file
char* file_name_generator(int num) {
	char* ret = malloc(20);
	sprintf(ret, "data%i.csv", num);
	return ret;
}

// this function will write table under the file name filename.csv
void write_to_file(char* filename, Table table) {
	int size_of_output = 0;
	// get size of column name
	char** name_list = malloc(table.col_count * sizeof(char*));
	for (int i = 0; i != ((int) table.col_count); ++i) {
        size_of_output += strlen(full_name(current_db->name, table.name, table.columns[i].name));
    	name_list[i] = malloc(strlen(full_name(current_db->name, table.name, table.columns[i].name))+1);
    }
    

    // store column names in a list
    
    for (int i = 0; i != ((int)table.col_count); ++i) {
    	name_list[i] = full_name(current_db->name, table.name, table.columns[i].name);
 
    }

	// WRITE THE COLUMN NAMES TO FILE
    FILE* fp;

    fp = fopen(filename, "w");
 
    fprintf(fp, "%s\n", place_commas(name_list,table.col_count, size_of_output));
    //fflush(fp);

    // WRITE THE DATA
    for (int i = 0; i != ((int) table.table_length); ++i) {
	    for (int j = 0; j != ((int) table.col_count); ++j) {

	    	if (j == (((int)table.col_count)-1)) {
	    		fprintf(fp, "%i\n", table.columns[j].data[i]);
	    	} else {
	    		fprintf(fp, "%i,", table.columns[j].data[i]);
	    	}
	    }
	}
    fclose(fp);
    free(name_list);
}



void shutdown_server(void) {
    char** file_names = malloc(sizeof(char*) * current_db->tables_size);
    FILE* fp_cat;
    // open catalogue in write mode to list number of active tables and table names
    fp_cat = fopen("catalogue.txt", "w");
    fprintf(fp_cat, "%zu\n", current_db->tables_size);
    for (int i = 0; i != ((int) current_db->tables_size); ++i) {
    	file_names[i] = malloc(30);
    	file_names[i] = file_name_generator(i);
    	if (i == (((int)current_db->tables_size)-1)) {
    		fprintf(fp_cat, "%s,\n", file_names[i]);
    	} else {
    		fprintf(fp_cat, "%s,", file_names[i]);
    	}
    }

    fclose(fp_cat);

    if (!current_db->partial_shutdown) {
    	int ind_exists = 0;

	    for (int i = 0; i != (int)current_db->tables_size; i++) {
	    	ind_exists += (current_db->tables[i].num_idx > 0) ? 1:0;
	    }
	    FILE* fp_index = NULL;

	    if (ind_exists > 0){ 
	   		fp_index = fopen("index.txt", "w");
	 	}   

	 	// writing indexes to file; should change and write each index data out in disk
	    for (int i = 0; i != (int)current_db->tables_size; i++) {
	    	if (current_db->tables[i].num_idx > 0) {
	    		for (int j = 0; j != (int)current_db->tables[i].col_count; j++) {
	    			char* col_full = full_name(current_db->name, current_db->tables[i].name, current_db->tables[i].columns[j].name);
	    			if (current_db->tables[i].columns[j].index) {
	    				if (current_db->tables[i].columns[j].type == BTREE) {
	    					if (current_db->tables[i].columns[j].clustered) {
	    						fprintf(fp_index, "create(idx,%s,btree,clustered);", col_full);
	    					} else {
	    						fprintf(fp_index, "create(idx,%s,btree,unclustered);", col_full);
	    					}
	    				} else {
	    					if (current_db->tables[i].columns[j].clustered) {
	    						fprintf(fp_index, "create(idx,%s,sorted,clustered);", col_full);
	    					} else {
	    						fprintf(fp_index, "create(idx,%s,sorted,unclustered);", col_full);
	    					}
	    				}
	    			}
	    			
	    		}
	    	} else {
	    		//fprintf(fp_index, "\n");
	    	}
	    }
	    if (ind_exists > 0) {
	    	fclose(fp_index);
		}

	    for (int i = 0; i != ((int) current_db->tables_size); ++i) {
	    	write_to_file(file_names[i], current_db->tables[i]);
	    }

	    free(file_names);

	    for (int i = 0; i != ((int) current_db->tables_size); ++i) {
	    	for (int j = 0; j != ((int)current_db->tables[i].col_count); ++j) {
	    		free(current_db->tables[i].columns[j].data);
	    	}
	    	free(current_db->tables[i].columns);
	    }

	    free(current_db->tables);

	    free(current_db);
	} else {
		write_to_file(file_names[((int) current_db->tables_size)-1], current_db->tables[((int) current_db->tables_size)-1]);
	    current_db->partial_shutdown = false;
	}


}

Status load_columns(Table* table, int* vals, int index, int offset, int size) {
	if (table->columns + index) {
		memcpy(table->columns[index].data + offset, vals, size);

	}
	 
	Status ret_status;
	ret_status.code = OK;

	if (index == 0) {

		table->table_length += (size/4);	
	}
	 
	return ret_status;

}

// THIS IS WHERE MULTITHREADING FOR BATCH QUERIES TAKES PLACE

// this is my thread pool
pthread_t tpool[THREADS];
thread_in thread_args[THREADS];
DbOperator* op_quer[500];
ClientContext* glob_context;
int glob_socket;



// num to execute is the index in query_batch
void* select_per_thread(void* input) {
	message send_message;
	thread_in* thread_input = (thread_in*) input;
    int per_thread = current_db->num_in_batch/THREADS;

    int max = 0;
    // starting index to write to in chandle table
    int chandle_start = thread_input->context->chandles_in_use+thread_input->num_to_execute;
     
   
    max = (thread_input->num_to_execute) + per_thread;
   
    for (int i = thread_input->num_to_execute; i != max; i++) {
    	DbOperator* query = parse_command(current_db->query_batch[i+1], &send_message, thread_input->client_socket, thread_input->context);
    	SelectOperator operator = query->operator_fields.select_operator;
    	 
    	selectt(thread_input->context->chandle_table, chandle_start, operator.table, operator.column,
                         operator.interm, operator.lower, operator.upper);
    	chandle_start++;
    }
    return NULL;
}

void dispatch_threads(int client_socket, ClientContext* context) {
 
	int per_thread = current_db->num_in_batch/THREADS;
 
 	// iterate through my thread pool and give each thread a chunk of work to do
    for (int i = 0; i != THREADS; i++) {
        thread_args[i].num_to_execute = i*per_thread;
        thread_args[i].context = context;
        thread_args[i].client_socket = client_socket;
 
        pthread_create(tpool + i, NULL, &select_per_thread, (void*) (thread_args + i));;
    }
  //  context = glob_context;
}

// waits for all threads to finish executing before continuing
void wait_all() {
    for (int i = 0; i < THREADS; i++) {
        pthread_join(tpool[i], NULL);
   
    }
}



