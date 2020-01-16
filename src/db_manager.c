#define _DEFAULT_SOURCE
#include "cs165_api.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "client_context.h"
#include "utils.h"
#include "parse.h"
// In this class, there will always be only one active database at a time
Db *current_db=NULL;

int global_counter1 = 0;
int global_table_length = 0;
void array_AND(int* store, int* comp, int num_vals) {
	for (int i = 0; i != num_vals; i++) {
		// printf("orig value: %i\n", store[i]);
		// printf("other value: %i\n", comp[i]);
		store[i] |= comp[i];
	}
}

void delete_update(Table* tbl, Result* res) {
	 	//printf("line 23 db.c: %i\n", tbl->table_length);
		array_AND(tbl->chandle_deletes->payload, res->payload, tbl->table_length);

 
}
	// 	if (res->select_type == BIT) {
	// 		if (!tbl->chandle)
	// 		tbl->chandle_deletes = res;
	// 	} else if (res->select_type == IDX) {
	// 		memset((void*) tbl->chandle_deletes->payload, 1, res->orig_lngth*sizeof(int));
	// 		for (int i = 0; i < res->num_tuples; i++) {
	// 			tbl->chandle_deletes->payload[res->payload[i]] = 0;	 
	// 		}

	// 	} else {
	// 		Column* col = NULL;
	// 		memset((void*) tbl->chandle_deletes->payload, 1, tbl->table_length*sizeof(int));
	// 		for (int i = 0; i < tbl->col_count;i++) {
	// 			if (tbl->columns[i].clustered) {
	// 				col = columns[i].clustered;
	// 				break;
	// 			}
	// 		}

	// 		for (int j = col->index->sorted[res->payload[0]]; j < col->index->sorted[res->payload[1]]; j++) {
	// 			int id = col->index->sorted[j].id;
	// 			tbl->chandle_deletes->payload[id] = 0;
	// 		}
	// 	}
	// } else {
	// 	if (res->select_type == BIT) {
	// 		array_AND(tbl->chandle_deletes->payload, res->payload, tbl->table_length);

	// 	} else if (res->select_type == IDX) {
	// 		for (int i = 0; i != res->num_tuples;i++) {
	// 			tbl->chandle_deletes->payload[res->payload[i]] = 0;
	// 		}
	// 	} else {
	// 		Column* col = NULL;
	// 		memset((void*) tbl->chandle_deletes->payload, 1, tbl->table_length*sizeof(int));
	// 		for (int i = 0; i < tbl->col_count;i++) {
	// 			if (tbl->columns[i].clustered) {
	// 				col = columns[i].clustered;
	// 				break;
	// 			}
	// 		}
	// 	}
	// }
//}

void update(Column* column, Table* table, Result* result, int value) {
	
	printf("line 17 db.c: %s\n", result->nm);
	if (result->select_type == IDX) {
		printf("line 19 idx db.c\n");
		for (int i = 0; i != result->num_tuples; i++) {
			column->data[result->payload[i]] = value;
		}
	} else if (result->select_type == BIT) {
		printf("line 24 db.c\n");
		for (int i = 0; i != result->orig_lngth; i++) {
			if (result->payload[i] == 1) {
				column->data[i] = value;
			}
		}
	} else if (result->select_type == RANGE) {
		printf("in update line 29 db.c\n");
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
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
long long _sum(Result* result) {
	long long count = 0;

	for (int i = 0; i != ((int)result->num_tuples); ++i) {
		count += ((long long) result->payload[i]);
		//printf("count in sum fxn: %lli\n", count);
	}

	return count;
}

int _max(Result* result, int* res) {
	int cur_max = result->payload[0];
	for (int i = 1; i != ((int)result->num_tuples); ++i) {
		//printf("max fxn: %i\n", result->payload[i]);
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
	printf("TOTAL: %lli\n", total);
	printf("num tuples: %li\n", result->num_tuples);
	long double avgg = ((long double)(total))/((long double) result->num_tuples);
	printf("RESULTING AVG: %Lf\n", avgg);
	return avgg;
}

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

void do_arith(int ind_needed, char* operation, char* store_as , Result* column_for_op, Result* result_list) {
	//ind_needed+=1;
	printf("do_arith function col_name: %zu\n", column_for_op->num_tuples);
	int result_of_arith = 0;
	long long result_sum;
	int already_here = find_result(result_list, store_as, ind_needed);

	printf("line62 db.c: %i\n", ind_needed);
	if (already_here > -1) {
		ind_needed = already_here;
	}

	printf("line 66 db_manager.c: %i\n", ind_needed);
	long double result_avg;
	result_list[ind_needed].num_tuples = 1;
	strcpy(result_list[ind_needed].nm, store_as);
	int flag = 0;
	if (strncmp(operation, "avg", 3) == 0) {
		flag=1;
		result_avg = _avg(column_for_op);
		printf("result avggg : %f\n", ((float)result_avg));
		result_list[ind_needed].payload_float[0] = result_avg;
		result_list[ind_needed].data_type = FLOAT;
		result_list[ind_needed].select_type = OP;
	}
	else if (strncmp(operation, "sum", 3)==0) {
		flag=2;
		result_sum = _sum(column_for_op);
		//printf("result of sum: %li", result_sum);
		
	} else if (strncmp(operation, "min", 3)==0) {

		result_of_arith = _min(column_for_op);
		printf("result of sum: %i", result_of_arith);
	} else if (strncmp(operation, "max", 3)==0) {
		_max(column_for_op,&result_of_arith);
		printf("result of sum: %i", result_of_arith);
	}

	if (flag == 0) {
		printf("line 83 db_manager.c: %i\n", ind_needed);
		result_list[ind_needed].payload[0] = result_of_arith;
		result_list[ind_needed].data_type = INT;
		result_list[ind_needed].select_type = OP;
	} else if (flag == 2) {
		printf("UNB FLAG IS 2\n");
		result_list[ind_needed].payload_long[0] = result_sum;

		result_list[ind_needed].data_type = LONG;
		result_list[ind_needed].select_type = OP;
	}

	

}

void fetch(Result* res, int ind_needed, Table* table, Column* column, char* interm, Result* intermediate) {
	if (!column) {
		printf("jsld\n");
	}
	printf("got to the fetch function\n");
	//column = &table->columns[table->counter];


	int lookup = find_result(res, interm, ind_needed);
	if (lookup > -1) {
		ind_needed = lookup;
	} 
	printf("way1\n");
	//int final_payload[intermediate->num_tuples];
	int num_stored = 0;

	printf("TABLE LENGTH IN SELECT: %zu\n", intermediate->num_tuples);
	for (int i = 0; i != ((int) table->table_length); ++i) {
		//printf("VAL OF BITVECTOR: %i\n", table->chandle_deletes->payload[i]);
		if (intermediate->payload[i] == 1 && table->chandle_deletes->payload[i] == 0) {
		//	printf("Successful storage\n");
		//	printf("val: %i\n",column->data[i]);
			res[ind_needed].payload[num_stored] = column->data[i];
			num_stored++;
			//printf("db: %s\n", intermediate->nm);
			//printf("db2: %i\n", num_stored);
		}
	}
	printf("way3 number stored in orig fetch: %i\n", num_stored);
	res[ind_needed].num_tuples = num_stored;
	printf("way4\n");
	res[ind_needed].data_type = INT;
	res[ind_needed].select_type = FTH;
	printf("interm: %s\n", interm);
	strcpy(res[ind_needed].nm, interm);
	printf("way6\n");
//	res[ind_needed].payload = malloc(sizeof(int)* intermediate->num_tuples);
//	res[ind_needed].payload = final_payload;

}

int _printt(Result* result, int iterate_til, char* name_of_print) {
	for (int i = 0; i < iterate_til; ++i) {
		//printf("name of result in print: %s\n", result[i].nm);
		if (strcmp(result[i].nm, name_of_print) == 0) {
			//printf("number of results in print: %zu \n", result[i].num_tuples);
			for (int j = 0; j < ((int) result[i].num_tuples); ++j) {
				printf("%i\n", result[i].payload[j]);
			} 
		
			return i;
		}
	}
	return -1;

}


void _select2(Result* res_bitvector, Result* res_for_range, Result* res, 
				int num_results, char* name, int lower, int upper)
{
	int lookup = find_result(res, name, num_results);
	if (lookup > -1) {
		num_results = lookup;
	}

	int number_of_qual = 0;
	int num_ones = 0;
	//printf("table length: %zu\n", table->table_length);
	//column = &table->columns[table->counter];
	if (res_bitvector->select_type == BIT) {
		for (int i = 0; i != res_bitvector->orig_lngth; ++i) {
			//printf("res_bitvector num tuples line 191: %i\n", res_bitvector->num_tuples);

			
					int tmp1 = (res_bitvector->payload[i] == 1) ? 1:0;
					int tmp2 = (res_for_range->payload[num_ones] >= lower && res_for_range->payload[num_ones] < upper ) ? 1:0;
					
						res[num_results].payload[i] = (tmp2*tmp1);
						number_of_qual+=tmp2;

				

				num_ones+=tmp1;
			//printf("VALUE IN BITVECTOR: %i\n", res[index_needed].payload[i]);
		}
		res[num_results].select_type = BIT;
	} else {
		//printf("within the idx select2: %i\n", res_for_range->num_tuples);
		//int num_of_e = 0;
		for (int i = 0; i < (int)res_for_range->num_tuples; i++) {
			// printf("LOWER: %i\n", lower);
			// printf("UPPER: %i\n", upper);
			//printf("value: %i\n", res_bitvector->payload[0]);

			if (res_for_range->payload[i] >= lower && res_for_range->payload[i] < upper) {
				res[num_results].payload[number_of_qual] = res_bitvector->payload[0] + i;
				number_of_qual++;
			}

			res[num_results].select_type = IDX2;

		}
	}
		printf("number of qual: %i\n", number_of_qual);
		res[num_results].num_tuples=number_of_qual;

		

		res[num_results].nm[0] = '\0';
		strcpy(res[num_results].nm, name);
		
		res[num_results].data_type=INT; 
		
	}


Result* selectt(Result* res, int index_needed, Table* table, Column* column, char* interm, int lower, int upper) {
	//int bitvect[table->table_length];
	printf("yoyoyo\n");
	// 
	// if (!column) {
	// 	printf("column is null");
	// }

	// if (!(column->data)) {
	// 	printf("no data");
	// }


	int lookup = find_result(res, interm, index_needed);
	if (lookup > -1) {
		index_needed = lookup;
	} 
	
	int num_ones = 0;
	printf("table length: %zu\n", table->table_length);
	//column = &table->columns[table->counter];
	printf("COLNAMEM : %s\n", column->name);
	for (int i = 0; i != ((int) table->table_length); ++i) {
		// printf("LOWER: %i\n", lower);
		// printf("UPPER: %i\n", upper);
		// printf("VAL: %i\n", column->data[i]);
		
		// if ((column->data[i] >= lower && column->data[i] < upper)) {
		// 	res[index_needed].payload[i] = 1;
		// 	num_ones++;
		// } else {
		// 	res[index_needed].payload[i] = 0;
		// }
		int tmpp = (column->data[i] >= lower && column->data[i] < upper) ? 1:0;
		//printf("index needed: %i\n", index_needed);
		res[index_needed].payload[i] = tmpp;
		//printf("STORED: %i\n", res[index_needed].payload[i]);
		num_ones+=tmpp;
		
		
	}
	//printf("huh1\n");

	//int* payld = bitvect;

	// MALLOC THIS
	//Result* res = {1, INT, interm, bitvect};
//	printf("huh2\n");
	
	printf("huh");
	
	//Result res2;
	printf("NUM TUPLES: %i\n", num_ones);
	res[index_needed].num_tuples=num_ones;
	printf("huh3\n");
	// if (res) {
	// 	printf("f%p\n", *res);
	// }
	//res[index_needed].nm = malloc(MAX_SIZE_NAME);
	
	printf("line 217 in db_manager.c: %s\n", interm);

	res[index_needed].nm[0] = '\0';
	strcpy(res[index_needed].nm, interm);
	
	printf("index needed value line 222 db_manager: %i\n", index_needed);
	printf("line 223:%s\n", res[index_needed].nm);
	

	res[index_needed].data_type=INT;
	res[index_needed].select_type = BIT;
	res[index_needed].orig_lngth = table->table_length;
	
	
//	res[index_needed].payload = payld;
	printf("select fxn: %s\n", interm);
	
	return res; 

	// my issue is in relational insert 
	// where I assign the data and hash the column
	// table hash seems to be working though

}

Status relational_insert(Table* table, int* values) {
	size_t num_cols = table->col_count;
	//size_t cur = table->table_length;
	printf("numb cols: %i", global_table_length);
	Status ret_status;
	printf("you8\n");
	// if (num_cols != length(values))


	size_t num_vals = table->table_length;


	if (table->table_length == 0) {
		for (int i = 0; i != ((int) num_cols); ++i) {
			//int* dat;
			// if (!(table->columns[i])) {
			// 	printf("whyyy\n");
			// }
			printf("yes3\n");
			// if (!(table->columns[i].data)) {
			// 	printf("yes4\n");
			// 	int* dat = (int*) malloc(sizeof(int));
			// 	dat = values[i];
			// } else {
			// 	int* dat = realloc(table->columns[i].data, sizeof(int));
			// 	dat[(int) cur] = values[i];
			// }
			printf("yes5\n");
			

			printf("yes\n");
			
			table->columns[i].data = (int*) malloc(1000*sizeof(int));
			
			printf("num vals: %zu \n", num_cols);
		    
		}	
	}

		
	for (int i = 0; i != ((int)num_cols); ++i) {
 
			table->columns[i].data[num_vals] = values[i];
		 
		//printf("This is the values passed: %i", values[i]);
	}

	printf("rel_insert col_name: %s\n", table->columns[0].name);
	
	printf("you9\n");
	table->table_length++;
	global_table_length++;
	printf("tbl len rel insert: %i\n", global_table_length);

	ret_status.code = OK;

	return ret_status;


}
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
	printf("what3\n");
	if (strlen(name) > MAX_SIZE_NAME) {
		ret_status.code = ERROR;
		ret_status.error_message = "Name too long";
		return ret_status;
	}
	printf("what4\n");

	// if (!(table->columns)) {
	// 	new_col = (Column *) malloc(sizeof(Column));
	// } else {
	// 	new_col = (Column*) realloc(table->columns, sizeof(Column));
	// }
	printf("what5\n");
	//size_t index = table->counter;
	printf("what6\n");
	
	
	if (!table) {
		printf("f\n");
	}
	//table->columns = malloc(sizeof(Column) * 6);
	
	strcpy(table->columns[global_counter1].name, trim_quotes(name));
	printf("yuhhhh: %s \n", table->columns[global_counter1].name);
	table->columns[global_counter1].data = (int*) malloc(sizeof(int)*10000000);
	table->columns[global_counter1].index = NULL;
	table->columns[global_counter1].type = NOIDX;
	table->columns[global_counter1].num_in_pool = 0;
	//table->counter++;
	global_counter1++;
	printf("what8 index: %i\n", global_counter1);

	// hash full column name
	
	ret_status.code=OK;
	table->counter++;
	return ret_status;
}

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning

	// check that table name isn't too long
	printf("yuh0\n");
	if (strlen(name) > MAX_SIZE_NAME) {
		ret_status->code = ERROR;
		ret_status->error_message = "Name too long";
		return NULL;
	}
	global_table_length=0;
	global_counter1 = 0;
	// create table and columns
	printf("yuh1\n");

	printf("yuh2\n");
	Table* table = malloc(sizeof(Table));



	// if (!(db->tables)) {
	// 	printf("yuh4\n");
	//table = (Table*) malloc(sizeof(Table)*10);
	// 	table->counter = 0;
	// } else {
	// 	// if a table already exists add a table to current database
	//  	table = (Table*) realloc(db->tables, sizeof(Table));
	// }
	printf("yuh5\n");
	int index = lookup_table((char*)name);
	bool increment = false;
	if (index == -1) {
		index = db->tables_size;
		increment = true;
	}
	//size_t index = db->tables_size;
	printf("THE TABLE SIZE IS IN CREATE TABLE: %u", index);

	printf("yuh6\n");
	strcpy(current_db->tables[index].name, name);
	printf("table name in create: %s\n", table[index].name);
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

	//printf("create_table col num: %zu\n", table[index].col_counst);

	printf("yuh7\n");
	//&(db->tables[current_db->tables_size]) = table;
	if (increment) {
		current_db->tables_size++;
	}
	//printf("n: %s\n", db->tables[0].name);
	ret_status->code=OK;

	printf("size of current_db: %zu\n", current_db->tables_size);

	// unsigned int indx = hash(name);
	// table_lookup[indx] = table; 
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
    printf("line9 numnelts: %i\n", num_elts);
    for (int i = 0; i != num_elts-1; ++i) {
    	//printf("The first thing: %s\n", string_list[i]);
        strcpy(out_buf+index, string_list[i]);
        index += strlen(string_list[i]);
        strcpy(out_buf+index, ",");
        index+=1;
    }
    printf("line10\n");
    strcpy(out_buf+index, string_list[num_elts-1]);
    printf("line12\n");
    out_buf[size_of_output+num_elts-1] = '\0';
    printf("line11\n");
    char* ret_buf = malloc(size_of_output+num_elts);
    strcpy(ret_buf, out_buf);
    printf("line1w\n");
    return ret_buf; 


}

// create the name of a data file
char* file_name_generator(int num) {
	char* ret = malloc(20);
	sprintf(ret, "data%i.csv", num);
	return ret;
}

void write_to_file(char* filename, Table table) {
	int size_of_output = 0;
	// get size of column name
	char** name_list = malloc(table.col_count * sizeof(char*));
	for (int i = 0; i != ((int) table.col_count); ++i) {
        size_of_output += strlen(full_name(current_db->name, table.name, table.columns[i].name));
    	name_list[i] = malloc(strlen(full_name(current_db->name, table.name, table.columns[i].name))+1);
    }
    printf("whats2\n");
    

    // store column names in a list
    printf("number of cols: %zu", table.col_count);
    for (int i = 0; i != ((int)table.col_count); ++i) {
    	name_list[i] = full_name(current_db->name, table.name, table.columns[i].name);
    	//printf("line5: %s\n", name_list[i]);
    }

	// WRITE THE COLUMN NAMES TO FILE
    FILE* fp;

    fp = fopen(filename, "w");
    printf("nameoffile: %s\n", filename);
    //fflush(fp);
    printf("yyyyyypl : \n");
    printf("WRITING TO FILE IN DB_M.C: %s\n", filename);
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
	// printf("line1\n");
	// int num_tables = current_db->tables_size;
	// printf("line2\n");
	// int size_of_output = 0;
	// printf("line3\n");
 //    for (int i = 0; i != current_db->tables[0].col_count; ++i) {
 //        size_of_output += strlen(full_name(current_db->name, current_db->tables[0].name, current_db->tables[0].columns[i].name));
 //    }
 //    printf("line4\n");
 //    char** yes = malloc(size_of_output);
 //    for (int i = 0; i != current_db->tables[0].col_count; ++i) {
 //    	yes[i] = full_name(current_db->name, current_db->tables[0].name, current_db->tables[0].columns[i].name);
 //    	printf("line5: %s\n", yes[i]);
 //    }
    //printf("shutdown_server: %s\n", place_commas(yes,current_db->tables[0].col_count, size_of_output));
    char** file_names = malloc(sizeof(char*) * current_db->tables_size);
    FILE* fp_cat;
    fp_cat = fopen("catalogue.txt", "w");
    printf("whats3\n");
    fprintf(fp_cat, "%zu\n", current_db->tables_size);
    printf("whats4\n");
    for (int i = 0; i != ((int) current_db->tables_size); ++i) {
    	file_names[i] = malloc(30);
    	file_names[i] = file_name_generator(i);
    	//printf("whats5\n");
    	if (i == (((int)current_db->tables_size)-1)) {
    		fprintf(fp_cat, "%s,\n", file_names[i]);
    	} else {
    		fprintf(fp_cat, "%s,", file_names[i]);
    	}
    }

    fclose(fp_cat);

    if (!current_db->partial_shutdown) {
    	printf("WROMG\n");
	    int ind_exists = 0;

	    for (int i = 0; i != (int)current_db->tables_size; i++) {
	    	ind_exists += (current_db->tables[i].num_idx > 0) ? 1:0;
	    }
	    FILE* fp_index = NULL;

	    if (ind_exists > 0){ 
	   		fp_index = fopen("index.txt", "w");
	 	}   
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



	    //printf("whats1: %s\n", );
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
		printf("partial success\n");
		write_to_file(file_names[((int) current_db->tables_size)-1], current_db->tables[((int) current_db->tables_size)-1]);
	    current_db->partial_shutdown = false;
	}


}

Status load_columns(Table* table, int* vals, int index, int offset, int size) {
	// if (global_table_length == 0) {

	// 		int* dat;
	// 		// if (!(table->columns[i])) {
	// 		// 	printf("whyyy\n");
	// 		// }
	// 		printf("yes3\n");
	// 		// if (!(table->columns[i].data)) {
	// 		// 	printf("yes4\n");
	// 		// 	int* dat = (int*) malloc(sizeof(int));
	// 		// 	dat = values[i];
	// 		// } else {
	// 		// 	int* dat = realloc(table->columns[i].data, sizeof(int));
	// 		// 	dat[(int) cur] = values[i];
	// 		// }
	// 		printf("yes5\n");
			

	// 		printf("yes\n");
			
	// 		table->columns[index].data = (int*) malloc(1000*sizeof(int));
		
		    
		
	// }

	//printf("is this where the segfaul: %i\n", index);
	if (table->columns + index) {
		memcpy(table->columns[index].data + offset, vals, size);

	}
	//printf("name of table in: %s\n", table->columns[index].name);
	Status ret_status;
	ret_status.code = OK;

	if (index == 0) {

		table->table_length += (size/4);	
	}
	//printf("db_manager: global_table_length: %zu\n", table->table_length);
	return ret_status;

}




#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
 
#define THREADS 2
 

pthread_t tpool[THREADS];
thread_in thread_args[THREADS];
DbOperator* op_quer[500];
ClientContext* glob_context;
int glob_socket;

// void fill_op_quer(message* send_message, int client_socket, ClientContext* context){
	 
// 	  for (int i = 0; i != current_db->num_in_batch; i++) {
	  		
//             DbOperator* op = parse_command(current_db->query_batch[i], &send_message, client_socket, context);       
//       		op_quer[i] = malloc(sizeof(op));
//       		memcpy(op_quer[i], op, sizeof(op));
//       		printf("line 757 db_m.c: %s\n", op_quer[i]->operator_fields.select_operator.table->name);
//        }
//        printf("line 757 db_m.c: %s\n", op_quer[0]->operator_fields.select_operator.table->name);
// }

// num to execute is the index in query_batch
void* select_per_thread(void* input) {
	message send_message;
	thread_in* thread_input = (thread_in*) input;
    int per_thread = current_db->num_in_batch/THREADS;

    int max = 0;
    // starting index to write to in chandle table
    int chandle_start = thread_input->context->chandles_in_use+thread_input->num_to_execute;
    //printf("CHANDLE_START LINE 757 DB_MAN.C: %i\n", chandle_start);
   
    max = (thread_input->num_to_execute) + per_thread;
   // printf("This is the value of max: %i\n", max);
    for (int i = thread_input->num_to_execute; i != max; i++) {
    	DbOperator* query = parse_command(current_db->query_batch[i+1], &send_message, thread_input->client_socket, thread_input->context);
    	SelectOperator operator = query->operator_fields.select_operator;
    	//printf("NAME OF TBL: %s\n", operator.table->name);
    	selectt(thread_input->context->chandle_table, chandle_start, operator.table, operator.column,
                         operator.interm, operator.lower, operator.upper);
    	chandle_start++;
    }
    return NULL;
}

void dispatch_threads(int client_socket, ClientContext* context) {
	//thread_in* input;
	//glob_context = malloc(sizeof(ClientContext)); 
	//glob_context = context;
	//glob_socket = client_socket;
	int per_thread = current_db->num_in_batch/THREADS;
	//printf("line 793 db_m.c num in batch: %i\n", current_db->num_in_batch);
    for (int i = 0; i != THREADS; i++) {
        thread_args[i].num_to_execute = i*per_thread;
        thread_args[i].context = context;
        thread_args[i].client_socket = client_socket;
       // printf("ARGUMENT TO PTHREAD LINE 775: %i\n", thread_args[i]);
        pthread_create(tpool + i, NULL, &select_per_thread, (void*) (thread_args + i));;
    }
  //  context = glob_context;
}

void wait_all() {
    for (int i = 0; i < THREADS; i++) {
        pthread_join(tpool[i], NULL);
        //printf("donep\n");
    }
}



