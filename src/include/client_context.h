#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

int lookup_table(char *name);
Table* table_lookup[2003];
Column* column_lookup[2003];
Result* result_lookup[2003];
int global_result_counter;

int glob_mes_to_receive;
int find_result(Result* result_table, char* result_name, int num_to_iter);
int find_column(char* table_name, char* col_name);

#endif
