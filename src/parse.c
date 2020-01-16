/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#include <limits.h>



/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    printf("token: %s\n", token);
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    printf("yooo\n");
    return token;
}

char* next_per(char** token) {
    char* tokenn = strsep(token, ".");
    return tokenn;
}

DbOperator* parse_delete(char* create_arguments, ClientContext* context) {
    DbOperator* dbo = malloc(sizeof(DbOperator));

    char** create_arguments_index = &create_arguments;

    char* tbl_name = strsep(create_arguments_index, ",");
    strsep(&tbl_name, ".");
    dbo->operator_fields.delete_operator.tbl = &current_db->tables[lookup_table(tbl_name)];
    dbo->operator_fields.delete_operator.res = &context->chandle_table[find_result(context->chandle_table, create_arguments, context->chandles_in_use)];
    printf("table name in del parse: %s,%i\n", current_db->tables[lookup_table(tbl_name)].name, dbo->operator_fields.delete_operator.tbl->table_length);
    printf("value of index: %s,%i,%s\n", tbl_name,lookup_table(tbl_name),current_db->tables[5].name);
    if (strcmp(tbl_name, current_db->tables[5].name) == 0) {
        printf("does match\n");
    } else {
        printf("does not match\n");
    }
    dbo->type = DELETE;
    return dbo;

}

DbOperator* parse_update(char* create_arguments, ClientContext* context) {
    DbOperator* dbo = malloc(sizeof(DbOperator));
    char** create_arguments_index = &create_arguments;

    char* full_col = strsep(create_arguments_index, ",");
    strsep(&full_col, ".");
    char* tbl_name = strsep(&full_col, ".");
    printf("parse.c line 52: %s,%s\n", tbl_name, full_col);
    Table* tbl = &current_db->tables[lookup_table(tbl_name)];
    Column* col = &tbl->columns[find_column(tbl_name, full_col)];
    char* res_name = strsep(create_arguments_index, ",");
    printf("parse.c line 56: %s\n", res_name);
     dbo->operator_fields.update_operator.result = &context->chandle_table[find_result(context->chandle_table, res_name, context->chandles_in_use)];

    printf("parse.c line 59 %s\n", create_arguments);

    dbo->operator_fields.update_operator.column = col;
   
    dbo->operator_fields.update_operator.value = atoi(create_arguments);
    dbo->type = UPDATE;
    dbo->operator_fields.update_operator.tbl = tbl;

    return dbo;

}



DbOperator* parse_join(char* create_arguments, char* handle, ClientContext* context) {
    DbOperator* dbo = malloc(sizeof(DbOperator));
    char** create_arguments_index = &create_arguments;

    char* f1 = strsep(create_arguments_index, ",");
    char* s1 = strsep(create_arguments_index, ",");
    char* f2 = strsep(create_arguments_index, ",");
    char* s2 = strsep(create_arguments_index, ",");
    char* method = strsep(create_arguments_index, ",");
    char* name1 = strsep(&handle, ",");
    char* name2 = handle;

    int f1_ind = find_result(context->chandle_table, f1, context->chandles_in_use);
    int s1_ind = find_result(context->chandle_table, s1, context->chandles_in_use);
    int f2_ind = find_result(context->chandle_table, f2, context->chandles_in_use);
    int s2_ind = find_result(context->chandle_table, s2, context->chandles_in_use);

    dbo->operator_fields.join_operator.fetch1 = &context->chandle_table[f1_ind];
    dbo->operator_fields.join_operator.sel1 = &context->chandle_table[s1_ind];
    dbo->operator_fields.join_operator.fetch2 = &context->chandle_table[f2_ind];
    dbo->operator_fields.join_operator.sel2 = &context->chandle_table[s2_ind];

    dbo->operator_fields.join_operator.store_as1 = name1;
    dbo->operator_fields.join_operator.store_as2 = name2;

    dbo->operator_fields.join_operator.method = method;
    dbo->type = JOIN;
    printf("result: %s\n", f1);

    printf("result: %s\n", s1);

    printf("result: %s\n", f2);

    printf("result: %s\n", s2);


    printf("result: %s\n", name1);

    printf("result: %s\n", name2);

    return dbo;
}




DbOperator* parse_binop(char* create_arguments, char* handle) {
    message_status status = OK_DONE;
    DbOperator* dbo = malloc(sizeof(DbOperator)); 

    strncpy((char*) (dbo->operator_fields.binop_operator.op), create_arguments,3);
    dbo->operator_fields.arith_operator.op[3] = '\0';
    create_arguments += 3;
    char** create_arguments_index = &create_arguments;
    char* col1_name = next_token(create_arguments_index, &status);
    char* col2_name = next_token(create_arguments_index, &status);   
    dbo->operator_fields.binop_operator.store_as = handle;
    dbo->operator_fields.binop_operator.col1_name = trim_parenthesis(col1_name);
    dbo->operator_fields.binop_operator.col2_name = trim_parenthesis(col2_name);
    dbo->type = BINOP;
    return dbo;
}

DbOperator* parse_arith(char* create_arguments, char* handle) {
    message_status status = OK_DONE;
    DbOperator* dbo = malloc(sizeof(DbOperator)); 
    strncpy(dbo->operator_fields.arith_operator.op, create_arguments,3);
    dbo->operator_fields.arith_operator.op[3] = '\0';
    create_arguments += 3;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
       
    dbo->operator_fields.arith_operator.store_as = handle;
    dbo->operator_fields.arith_operator.col_name = trim_parenthesis(col_name);
    dbo->type = ARITH;
    return dbo;
}

DbOperator* parse_load(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    DbOperator* dbo = malloc(sizeof(DbOperator));    

    dbo->operator_fields.load_operator.file_name = trim_quotes(trim_parenthesis(col_name));
    dbo->type = LOAD;
    return dbo;

}
DbOperator* parse_print(char* create_arguments) {
    printf("here we are\n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->operator_fields.print_operator.num_to_print = 0;
    char** col_names = malloc(sizeof(char*)*10);

    while (*create_arguments_index) {

        char* col_name = next_token(create_arguments_index, &status);
        col_names[dbo->operator_fields.print_operator.num_to_print] = malloc(strlen(col_name)+1);
        (col_names[dbo->operator_fields.print_operator.num_to_print])[0] = '\0';
        strcpy(col_names[dbo->operator_fields.print_operator.num_to_print], trim_parenthesis(col_name));
        dbo->operator_fields.print_operator.num_to_print++;


    }


    dbo->type = PRINT;
    dbo->operator_fields.print_operator.names_to_load = col_names;
   // printf("parse print: %s\n",col_name);
    return dbo;
}




DbOperator* parse_create_column(char* create_arguments) {
    printf("yuh20\n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* tmp = next_token(create_arguments_index, &status);

    printf("this is temp: %c\n", tmp[0]);
    int count = 0;

    while (tmp[count] != '.') {
        ++count;
    }

    

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        printf("yuh21\n");
        return NULL;
    }

    char d_name[count+1];
    strncpy(d_name, tmp, count);
    char tabl_name[strlen(tmp) - count-3];
    strncpy(tabl_name, tmp + count+1, strlen(tmp) - count-2);
    d_name[count] = '\0';
    tabl_name[strlen(tmp)-count-2] = '\0';
    char* table_name = tabl_name;
    char* db_name = d_name;
    printf("%s\n", db_name);
    printf("%s\n", table_name);

    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(table_name);
    // if (table_name[last_char] != ')') {
    //     return NULL;
    // }
    // replace the ')' with a null terminating character. 
    table_name[last_char] = '\0';

    printf("%s\n", table_name);
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name: %s");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
  // make create dbo for table
    printf("man\n");
    DbOperator* dbo = malloc(sizeof(DbOperator));
    printf("man3");
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, col_name);
    dbo->operator_fields.create_operator.db = current_db;
    printf("man2\n");
    int count2 = 0;
    while (strcmp((current_db->tables)[count2].name, table_name) != 0 && count2 != 10 ) {
        count2++;
    }
    int ind_tbl = lookup_table(table_name);

    if (ind_tbl == -1) {
        return NULL;
    }
    dbo->operator_fields.create_operator.table = &current_db->tables[ind_tbl];

    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/


DbOperator* parse_create_tbl(char* create_arguments) {
    printf("yuh20\n");
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
     char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
   
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        printf("yuh21\n");
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    printf("line 184: %s\n", current_db->name);
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    printf("current_db in parse.c line 201: %s\n", current_db->name);
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/


DbOperator* parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}

DbOperator* parse_create_idx(char* create_arguments) {
    printf("entering parse_create_idx\n");
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        strsep(&token, ".");
        char* tbl_name = strsep(&token, ".");
        char* col_name = strsep(&token, ".");

        // find column
        int tbl_ind = lookup_table(tbl_name);
        int col_ind = find_column(tbl_name, col_name);

        if (tbl_ind == -1 || col_ind == -1) {
            printf("not successful with the lookup \n ");
            return NULL;
        }

        Table* tbl_of_interest = &(current_db->tables[tbl_ind]);
        Column* col_of_interest = &(tbl_of_interest->columns[col_ind]);


        token = strsep(&create_arguments, ",");
        if (token == NULL) {
            return NULL;
        }

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->operator_fields.index_operator.column_to_index = col_of_interest;
        dbo->operator_fields.index_operator.tbl = tbl_of_interest;
        if (strncmp(token, "btree", 5) == 0) {
            dbo->operator_fields.index_operator.type = BTREE;
        } else if (strncmp(token, "sorte", 5) == 0) {
            dbo->operator_fields.index_operator.type = SORTED;
        } else {
            printf("neither option for type line 314 parse.c: %s\n", token);
            return NULL;
        }

        token = strsep(&create_arguments, ",");

        if (strncmp(token, "clustered", 9) == 0) {
            dbo->operator_fields.index_operator.is_clustered = true;
        } else if (strncmp(token, "uncluster", 9) == 0) {
            dbo->operator_fields.index_operator.is_clustered = false;
        } else {
            printf("line 325 no clustering: %s\n", token);
            return NULL;
        }


        dbo->type = INDEX;
        return dbo;
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments) {
    message_status mes_status = OK_DONE;
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = strsep(&tokenizer_copy, ",");
        if (mes_status == INCORRECT_FORMAT) {
            printf("what");
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                printf("why is this\n");
                dbo = parse_create_db(tokenizer_copy);
                printf("line 283\n");
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                printf("yuhh\n");
                dbo = parse_create_column(tokenizer_copy);
            } else if (strcmp(token, "idx") == 0) {
                printf("line 364 idx create\n");
                dbo = parse_create_idx(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
            
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}


DbOperator* parse_select(char* query_command, message* send_message, char* intermediate, ClientContext* context) {
    //unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SELECT;

    if (strncmp(query_command, "(", 1) == 0) {
        printf("you1\n");
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* col_name = strsep(command_index, ",");
        printf("you2\n");
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }



        // int tablenm_len = 0;

        // while (col_name[tablenm_len] != '.') {
        //     tablenm_len++;
        // }

        // tablenm_len += 1;

        // int new_cnt = 0;

        // while (col_name[tablenm_len + new_cnt] != '.') {
        //     new_cnt++;
        // }

        // char name_of_table[new_cnt+1];

        // strncpy(name_of_table, col_name + tablenm_len, new_cnt);
        // name_of_table[new_cnt] = '\0';
        Table* table_needed = malloc(sizeof(Table));
        Column* select_column = NULL;
        char* check_if_col = strsep(&col_name, ".");

        if (!col_name) {
            dbo->operator_fields.select_operator.res_bitvector = &context->chandle_table[find_result(context->chandle_table, check_if_col,context->chandles_in_use)];
            char* name_of_res2 = strsep(command_index, ",");
            dbo->operator_fields.select_operator.res_for_range = &context->chandle_table[find_result(context->chandle_table, name_of_res2, context->chandles_in_use)];
            printf("This is the name of select result1 line 410: %s\n", dbo->operator_fields.select_operator.res_for_range->nm);
        } else {
            dbo->operator_fields.select_operator.res_for_range = NULL;
            dbo->operator_fields.select_operator.res_bitvector = NULL;
            char* name_of_table = strsep(&col_name, ".");
            char* name_of_col = strsep(&col_name, ".");
            printf("name_in_select: %s \n", name_of_col);
            printf("line 372 parse.c : %s\n", name_of_table);
            int tb_ind = lookup_table(name_of_table);
            printf("name of the table: %s\n", name_of_table);

            if (tb_ind == -1) {
                printf("table lookup failed!\n");
                return NULL;
            }
            //Table* table_needed = malloc(sizeof(Table));
            *table_needed =  current_db->tables[tb_ind];
             printf("parse_select seg fult\n");
            printf("name of table after lookup: %zu\n", table_needed->table_length);
            // lookup the table and make sure it exists.
            //Table* select_table = (char *) name_of_table; 
            //int s = find_column(name_of_table, name_of_col);
            int col_ind = find_column(name_of_table, name_of_col);
            if (col_ind == -1) {
                return NULL;
            }
            select_column = &(table_needed->columns[col_ind]);
            if (!select_column) {
                printf("WHAT THE HECK\n");
            }
            printf("Parse select data val %s\n", select_column->name);
            printf("you4\n");
            
            if (select_column == NULL || table_needed == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            }

        }

        // make insert operator. 
      
        dbo->operator_fields.select_operator.table = table_needed;
        dbo->operator_fields.select_operator.column = select_column;
        printf("you4\n");
        dbo->operator_fields.select_operator.interm = intermediate;
        token = strsep(command_index, ",");
        if (strcmp(token,"null") == 0){
            dbo->operator_fields.select_operator.lower = INT_MIN;
        } else {
            dbo->operator_fields.select_operator.lower = atoi(token);
        }
     
        token = strsep(command_index, ",");
        if (strncmp(token,"null", 4) == 0){
            dbo->operator_fields.select_operator.upper = INT_MAX;
        } else {
            printf("parse selecttt: %s\n", token);
            dbo->operator_fields.select_operator.upper = atoi(token);
        }
   

        // parse inputs until we reach the end. Turn each given string into an integer. 
        // while ((token = strsep(command_index, ",")) != NULL) {
        //     int insert_val = atoi(token);
        //     printf("low_bound: %i\n", insert_val);
        //     //dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
        //     //columns_inserted++;
        // }
        printf("you5\n");
        // check that we received the correct number of input values
        // if (columns_inserted != insert_table->col_count) {
        //     send_message->status = INCORRECT_FORMAT;
        //     free (dbo);
        //     return NULL;
        // } 


        return dbo;
    } else {
        printf("you6\n");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}






// parse_fetch



DbOperator* parse_fetch(char* query_command, message* send_message, char* intermediate) {
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        printf("you1\n");
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* col_name = next_token(command_index, &send_message->status);
        printf("you2\n");
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        int tablenm_len = 0;

        while (col_name[tablenm_len] != '.') {
            tablenm_len++;
        }

        tablenm_len += 1;

        int new_cnt = 0;

        while (col_name[tablenm_len + new_cnt] != '.') {
            new_cnt++;
        }

        next_per(&col_name);
        char* name_of_table = next_per(&col_name);
        char* name_of_col = next_per(&col_name);
        printf("name_in_select: %s \n", name_of_col);
        // lookup the table and make sure it exists.
        char* select_table = (char *) name_of_table; 
        int tbl_indd = lookup_table(select_table);
        if (tbl_indd == -1) {
            return NULL;
        }
        Table* table_needed = &current_db->tables[tbl_indd];
        printf("PARSE.C table_length: %s\n", table_needed->name);

        int c_ind = find_column(select_table, name_of_col);
        if (c_ind == -1) {
            return NULL;
        }

        Column* select_column = &(table_needed->columns[c_ind]);
        printf("THIS IS THE COL  NAMEL : %s\n", select_column->name);
        if (select_column == NULL || table_needed == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        dbo->operator_fields.fetch_operator.table = table_needed;
        dbo->operator_fields.fetch_operator.column = select_column;
        printf("you4\n");
        dbo->operator_fields.fetch_operator.interm = intermediate;
        token = strsep(command_index, ",");
        dbo->operator_fields.fetch_operator.res_name = trim_parenthesis(token);
        printf("This should be aplus: %s\n", trim_parenthesis(token));
        // parse inputs until we reach the end. Turn each given string into an integer. 
        // while ((token = strsep(command_index, ",")) != NULL) {
        //     int insert_val = atoi(token);
        //     printf("low_bound: %i\n", insert_val);
        //     //dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
        //     //columns_inserted++;
        // }
        printf("you5\n");
        // check that we received the correct number of input values
        // if (columns_inserted != insert_table->col_count) {
        //     send_message->status = INCORRECT_FORMAT;
        //     free (dbo);
        //     return NULL;
        // } 
    return dbo;
    } else {
        printf("you6\n");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        printf("you1\n");
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        printf("you2\n");
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        //printf("TBL_NM: %s\n", next_per(&table_name));
        //printf("TBL_NM: %s\n", next_per(&table_name));

        next_per(&table_name);
        char* tbl_nm = next_per(&table_name);
        // lookup the table and make sure it exists.
        printf("db name insert: %zu\n", current_db->tables_size); 
        printf("tbl name insert: %s\n", tbl_nm); 
        int t_ind = lookup_table(tbl_nm);

        if (t_ind == -1) {
            return NULL;
        }
        Table* insert_table = &current_db->tables[t_ind];
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        printf("you99\n");
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        printf("num_cols: %s\n", insert_table->name);
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        printf("you4\n");
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        printf("you5\n");
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        printf("you6\n");
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator *dbo = malloc(sizeof(DbOperator));
    // if (global_result_counter == 1){
    //     printf("This is parse_command: %s\n", result_lookup[0]->nm);
    // }
    printf("got here lst\n");
    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        //cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    cs165_log(stdout, "QUERY: %i\n", (strncmp(query_command, "join", 4)));

    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command);
        if(dbo == NULL){
            send_message->status = INCORRECT_FORMAT;
        }
        else{
            send_message->status = OK_DONE;
        }
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        // include handle as argumenyt
        dbo = parse_select(query_command, send_message, handle, context);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, send_message, handle);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command);
    } else if ((strncmp(query_command, "max", 3) == 0) 
        || (strncmp(query_command, "min", 3) == 0) 
        || (strncmp(query_command, "avg", 3) == 0) 
        || (strncmp(query_command, "sum", 3) == 0)) {
      
        dbo = parse_arith(query_command, handle);
    } else if ((strcmp(query_command, "shutdown")) == 0) {
        // DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type=SHUTDOWN;
    } else if ((strncmp(query_command, "add", 3) == 0) || (strncmp(query_command, "sub",3) == 0)) {
        dbo = parse_binop(query_command, handle);
    } else if ((strcmp(query_command, "batch_queries()")) == 0) {
        dbo->type = BATCHQ;
    } else if ((strncmp(query_command, "join", 4) == 0)) {
        printf("line 783 parse.c\n");
        query_command+=4;
        dbo = parse_join(trim_parenthesis(query_command), handle, context);

    } else if ((strncmp(query_command, "relational_update", 17) == 0)) {
        query_command+=17;
        dbo = parse_update(trim_parenthesis(query_command), context);
    } else if (strncmp(query_command, "relational_delete", 17) == 0) {
        query_command+=17;
        dbo = parse_delete(trim_parenthesis(query_command), context);
    }

    printf("mannnnnn\n");
    if (dbo == NULL) {
        printf("what2\n");
        printf("line 753 null\n");
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
