/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#define _GNU_SOURCE
#define _DEFAULT_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include "time.h"


#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define SAMPLE_SIZE 1000



int global_result_count = 0;
/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 * 
 * Getting started hints: 
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/



float proxy_selectivity(int* data, int lower, int upper, int length) {
    int index = 0;
    int num_qual = 0;
    srand(time(NULL));
    for (int i = 0; i != SAMPLE_SIZE; i++) {
        index = rand() % (length+1);
        // printf("line 58 query optim: %i\n", index);
        // printf("contents: %i\n", data[i]);
        num_qual += ((data[index] >= lower && data[index] < upper) ? 1:0);
    }
    // printf("LENGTH: %i\n", length);
    // printf("NUM_QUAL: %i\n", RAND_MAX);

    return (((1.0)*num_qual)/SAMPLE_SIZE);

}


char* execute_DbOperator(DbOperator* query, int client_sock) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak. 
    // if (query->context->chandles_in_use == 0) {
    //     for (int i =0; i != 2003; ++i) {
    //         table_lookup[i] = NULL;
    //         column_lookup[i] = NULL;
             
    //     }
    //     //query->context->chandles_in_use = 0;
    //     global_result_count++;
    // }

    printf("yuh17\n");
    if(!query)
    {
        return "165";
    } else {
        printf("ABOUT TO INCREMENT!!!!");
        glob_mes_to_receive++;
    }
    printf("yuh18\n");
    if(query && query->type == CREATE){
        CreateOperator operator = query->operator_fields.create_operator;
        if(operator.create_type == _DB){
            printf("Line 74 server.c initi value of chandlesinuse: %i\n", query->context->chandles_in_use);
            char* db_name = operator.name;
            Status ret_status = create_db(db_name);

            if (ret_status.code == OK) {
                return "Database successfully created";
            } else {
                return ret_status.error_message;
            }


        }
        else if(operator.create_type == _TABLE){
            printf("yuh10\n");
            Status create_status;
            create_table(operator.db, 
                operator.name, 
                operator.col_count, 
                &create_status);
            printf("yuh11\n");
            if (create_status.code != OK) {
                return create_status.error_message;
            }
            return "Table successfully added";
        } else if (query->operator_fields.create_operator.create_type == _COLUMN) {
            printf("yuh27\n");
            printf("tb_name_server: %zu \n", operator.table->counter);
            Status ret = create_column(operator.name, operator.table);
            if (ret.code != OK) {
                return ret.error_message;
            }


            printf("tb_name_server: %zu \n", operator.table->counter);
            printf("name in server: %s\n", operator.table->columns[0].name);
            return "Column successfully added";
    }
    } else if (query && query->type == INSERT) {
        InsertOperator operator = query->operator_fields.insert_operator;


        Status ret = relational_insert(operator.table, operator.values);

        if (ret.code != OK) {
            return ret.error_message;
        } 
        
        // printf("length of table is now: %i\n", )
        // for (int i = 0; i < (int)operator.table->table_length;i++) {
        //     operator.table->columns[i].num_in_pool++;
        // }

        return "Insertion Successful";
    } else if (query && query->type == SELECT) {
        printf("Line 123 server.c initi value of chandlesinuse: %i\n", query->context->chandles_in_use);
        printf("yuh20\n");
        printf("SERVER SELECT CURRENT DB: %zu\n", current_db->tables[0].table_length);
        SelectOperator operator = query->operator_fields.select_operator;
        current_db->off = true;
        // if (operator.column) {
        //     if (operator.column->num_in_pool > 0) {
                
        //         printf("yuh");
        //       //  handle_index(operator.column, operator.table, operator.column->type, operator.column->clustered);
        //         operator.column->num_in_pool = 0;
        //     }
        // } else {
        //     printf("column is null: \n");
        // }

       // (query->context->chandle_table)[global_result_counter] = (Result*) malloc(sizeof(Result));
        //query->context->chandle_table->nm = malloc(HANDLE_MAX_SIZE);
       if (operator.res_bitvector) {
            printf("line 164 server.c\n");
            _select2(operator.res_bitvector, operator.res_for_range, query->context->chandle_table, 
                    query->context->chandles_in_use, operator.interm, operator.lower, 
                    operator.upper);
       } else {
            float selectivity = proxy_selectivity(operator.column->data, operator.lower, operator.upper, operator.table->table_length);
            printf("SELECTIVITY IN SERVER.C: %f\n", selectivity);
            if (operator.column->clustered && !current_db->off) {
                printf("SELECT CLUSTERED\n");
                clustered_select(query->context->chandle_table, query->context->chandles_in_use, operator.table,
                 operator.column, operator.interm, operator.lower, operator.upper);
            } else if (!operator.column->clustered && (operator.column->type != NOIDX) && !current_db->off) {
                printf("SELECT UNCLUSTERED\n");
                unclust_select(query->context->chandle_table, query->context->chandles_in_use, operator.table,
                 operator.column, operator.interm, operator.lower, operator.upper);
            } else {
                printf("NORMAL SELECT: %i\n", operator.table->table_length);
                selectt(query->context->chandle_table, query->context->chandles_in_use, operator.table, operator.column,
                         operator.interm, operator.lower, operator.upper);
                 // unclust_select(query->context->chandle_table, query->context->chandles_in_use, operator.table,
                 // operator.column, operator.interm, operator.lower, operator.upper);
            }
       }
       
        printf("yuh22\n");
         //= result;
        //printf("hahaha: %s\n", result->nm);
        //char* haha = (char*) (result->nm);
        //char empty_str[strlen(result->nm)];
        //strcpy(empty_str, result->nm);
        //printf("This was the empty: %s\n", empty_str);
        //query->context->chandle_table[global_result_counter]->nm = empty_str;


        // strcpy(result_lookup[global_result_counter]->nm, result->nm);
        //query->context->chandle_table[global_result_counter]->payload = result->payload;
      //query->context->chandle_table[global_result_counter]->num_tuples = result->num_tuples;
        //query->context->chandle_table[global_result_counter]->data_type = INT;
        //printf("name in result: %s\n", result->nm);
        
        // if (strcmp(res->nm, "a_plus") == 0) {
        //     return "select successful";
        // }
        query->context->chandles_in_use++;
        printf("its not possible: %s\n", query->context->chandle_table[0].nm);
        return "select successful";        
    } else if (query && query->type == FETCH) {
        printf("help1\n");
        FetchOperator operator = query->operator_fields.fetch_operator;
        printf("    RESULT NAME IN FETCH: %s\n", operator.res_name);
        int need_res_ind = find_result(query->context->chandle_table, operator.res_name, query->context->chandles_in_use);

        // int needed_result_index = 0;

        // //printf("%i\n", result_lookup[0]->num_tuples);
        // for (int i = 0; i != global_result_counter; ++i) {
        //     printf("help5\n");
        //     char* comp_name = query->context->chandle_table[i].nm;
        //     printf("nnnnnn : %s\n", query->context->chandle_table[i].nm);
        //     if (strcmp(comp_name, operator.res_name) == 0) {
        //         printf("help4\n");
        //         needed_result_index = i;
        //         break;
        //     }
        // }
        printf("help3\n");

        if (need_res_ind == -1) {
            return "fetch failure";
        }
        // Result* reslt = (Result *) malloc(sizeof(Result));
        printf("help3\n");
        if (query->context->chandle_table[need_res_ind].select_type == RANGE) {
            cluster_fetch(query->context->chandle_table, query->context->chandles_in_use, operator.table, 
            operator.column, operator.interm, &query->context->chandle_table[need_res_ind]);
        } else if (query->context->chandle_table[need_res_ind].select_type == IDX || query->context->chandle_table[need_res_ind].select_type == IDX2) {
            printf("id fetch\n");
            id_fetch(query->context->chandle_table, query->context->chandles_in_use, 
            operator.column, operator.table,operator.interm, &query->context->chandle_table[need_res_ind]);
        } else {
            fetch(query->context->chandle_table, query->context->chandles_in_use, operator.table, 
                operator.column, operator.interm, 
                &query->context->chandle_table[need_res_ind]);
        }
        query->context->chandles_in_use++;
        // for (int i = 0; i <= 20; ++i) {

        //     printf("This is the first id: %i\n", query->context->chandle_table[1].payload[i]);
    
        // }
        return "fetch complete";
    } else if (query && query->type == PRINT) {
        PrintOperator operator = query->operator_fields.print_operator;
        //int res = _printt(query->context->chandle_table, query->context->chandles_in_use, operator.name_to_load);
        for (int i = 0; i != operator.num_to_print; i++) {
            int res = find_result(query->context->chandle_table, operator.names_to_load[i], query->context->chandles_in_use);
            Result* result = query->context->chandle_table;
            if (res == -1) {
                return "print failure";
            }
            size_t total_to_send;
            if (result[res].data_type == LONG || result[res].data_type == FLOAT) {

               // printf("NUM TO PRINT: %i\n", operator.num_to_print);
               // printf("NUM TUPLES: %li\n", result[res].num_tuples);
             total_to_send = 8*result[res].num_tuples;
            } else {
                total_to_send = 4*result[res].num_tuples;
            }
            printf("This is num_tuples in server: %li\n", total_to_send);
            int* results_payload = result[res].payload;
            while (total_to_send > 0) {
               // printf("TOTAL TO SEND: %li\n", total_to_send);
                message send_message;
                if (total_to_send < 1024) {

                    send_message.length = total_to_send;
                } else {
                    send_message.length = 1024;    
                }
                
                int send_buffer[send_message.length + 1];
                memcpy(send_buffer, results_payload, send_message.length);
                
               // printf("send_buffer: %i\n", send_buffer[0]);

                if (result[res].data_type == INT) {
                    send_message.payload2 = send_buffer;

                    if (total_to_send < 1024 && i < (operator.num_to_print - 1)) {
                        send_message.status = OK_MULTPRINT;
                    } else if (total_to_send < 1024 && i == (operator.num_to_print-1)) {
                        send_message.status = OK_DONEP;
                    } else {
                        send_message.status = OK_INT;    
                    }
                    
                    if (send(client_sock, &(send_message), sizeof(message), 0) == -1) {
                        return "wtf";
                    }
           
                   // printf("yuh99: %lu\n", sizeof(send_buffer));

                
                    if (send(client_sock, send_message.payload2,send_message.length, 0) == -1) {
                        return "wtf2";
                    }
                    
                } else if (result[res].data_type == LONG ) {
                    send_message.payload4 = result[res].payload_long;
                 //   printf("server.c send long: %lli\n", send_message.payload4[0]); 
                    if (i < (operator.num_to_print - 1)) {
                        send_message.status = OK_LONG_MULTI;
                    } else {
                        send_message.status = OK_LONG;   
                    }
                   //send_message.length *=2;
                    if (send(client_sock, &(send_message), sizeof(message), 0) == -1) {
                        return "wtf";
                    }
           
                    printf("yuh99: %lu\n", sizeof(send_buffer));

                
                    if (send(client_sock, send_message.payload4, send_message.length, 0) == -1) {
                        return "wtf2";
                    }
                } else { 
                    send_message.payload3 = result[res].payload_float;
                    printf("The avg: %f", result[res].payload_float[0]);
                    if (i == (operator.num_to_print - 1)) {
                        send_message.status = OK_NEWLINE;
                    } else {
                        send_message.status = OK_FLOAT;
                    }
                    if (send(client_sock, &(send_message), sizeof(message), 0) == -1) {
                        return "wtf";
                    }
           
                    printf("yuh99: %lu\n", sizeof(send_buffer));

                
                    if (send(client_sock, send_message.payload3, send_message.length, 0) == -1) {
                        return "wtf2";
                    }
                }
                results_payload += (send_message.length/4);
                total_to_send -= send_message.length;
               // printf("total_to_send: %li\n", total_to_send);
        }
    }

        printf("ABOUT TO INCR 2");
        glob_mes_to_receive++;
        
        //recv(client_sock, &(result[res].payload[0]), sizeof(result[res].payload[0]), 0);
        //printf("print in server: %i\n", result[res].payload[0]);
        return "print completed";
    } else if (query->type == LOAD && query) {
        LoadOperator operator = query->operator_fields.load_operator;
        //void* recvm = malloc(sizeof(message));
        message send_message;
        current_db->wt = 0;
        send_message.length = strlen(operator.file_name);
        char send_buffer[send_message.length + 1];
        //memcpy(send_buffer, result[res].payload, 4*result[res].num_tuples);
        
        printf("send_buffer: %i\n", send_buffer[0]);
        send_message.payload = operator.file_name;
        printf("send_buffer: %s\n", send_message.payload);
        send_message.status = OK_LOAD;
        if (send(client_sock, &(send_message), sizeof(message), 0) == -1) {
            return "wtf";
        }
        printf("yuh99: %lu\n", sizeof(send_buffer));

        if (send(client_sock, send_message.payload, send_message.length, 0) == -1) {
            return "wtf2";
        }
        glob_mes_to_receive++;
        //size_t len;
 
    
        printf("This is the seg\n");
        return "Load Complete";
     

    } else if (query && query->type == ARITH) {
        ArithOperator operator = query->operator_fields.arith_operator;
        char buff[strlen(operator.col_name) + 1];
        strcpy(buff, operator.col_name);
        buff[strlen(operator.col_name)] = '\0';

        int need_res_ind = find_result(query->context->chandle_table, buff, query->context->chandles_in_use);


        if (need_res_ind == -1) {
            printf("Line 293 server.c\n");
            strsep(&operator.col_name, ".");
            printf("Line 295 server.c %s\n", operator.col_name);
            char* tbl_name = strsep(&operator.col_name, ".");

            printf("line 292 server.c tbl name: %s\n", tbl_name);
            int col_ind = find_column(tbl_name, operator.col_name);

            int tbl_ind = lookup_table(tbl_name);

            if ((col_ind == -1) || (tbl_ind == -1)) {
                return "arith failure";
            }
            need_res_ind = query->context->chandles_in_use;
            strcpy(query->context->chandle_table[need_res_ind].nm, operator.col_name);
            memcpy(query->context->chandle_table[need_res_ind].payload, current_db->tables[tbl_ind].columns[col_ind].data, current_db->tables[tbl_ind].table_length * 4);
            query->context->chandle_table[need_res_ind].data_type = INT;
            query->context->chandle_table[need_res_ind].num_tuples = current_db->tables[tbl_ind].table_length;
            query->context->chandles_in_use++;
            
        } 
        do_arith(query->context->chandles_in_use, ((char*) operator.op), operator.store_as, &(query->context->chandle_table[need_res_ind]), query->context->chandle_table);
        // for (int i = 0; i != (int)query->context->chandle_table[need_res_ind].num_tuples; ++i) {
        //     printf("server arith: %i\n", query->context->chandle_table[need_res_ind].payload[i]);
        // }
        query->context->chandles_in_use++;
        return "completed";
    }  else if (query && query->type == SHUTDOWN) {
        printf("got here\n");
        shutdown_server();


        free(query->context->chandle_table);
      
        
        return "shutdown";
    } else if (query && query->type == BINOP) {
         BinopOperator operator = query->operator_fields.binop_operator;
        int res1_ind = find_result(query->context->chandle_table, operator.col1_name, query->context->chandles_in_use);
        int res2_ind = find_result(query->context->chandle_table, operator.col2_name, query->context->chandles_in_use);

        if ((res1_ind == -1) || (res2_ind == -1)) {
            return "binop failure";
        }
        Result* res1 = &(query->context->chandle_table[res1_ind]);
        Result* res2 = &(query->context->chandle_table[res2_ind]);

        do_binop(query->context->chandles_in_use,(char*) operator.op, operator.store_as, 
            res1, res2, query->context->chandle_table);

        query->context->chandles_in_use++;
        
        return "binop done";
    } else if (query && query->type == BATCHQ) {
        return "batch_continue";
        
    } else if (query && query->type == INDEX) {
        IndexOperator operator = query->operator_fields.index_operator;

        printf("line 378 server.c col_name: %s\n", operator.column_to_index->name);
        operator.column_to_index->type = operator.type;
        operator.column_to_index->clustered = operator.is_clustered;
        operator.tbl->col_names_to_index[operator.tbl->index_create] = malloc(HANDLE_MAX_SIZE);
        strcpy(operator.tbl->col_names_to_index[operator.tbl->index_create], operator.column_to_index->name);
        operator.tbl->index_create++;

        handle_index(operator.column_to_index, operator.tbl, operator.type, operator.is_clustered);
        printf("after line 396 server.c\n");
        return "index created";

    } else if (query && query->type == JOIN) {
        JoinOperator operator = query->operator_fields.join_operator;
        printf("in the join op\n");
        if (operator.fetch1->num_tuples < operator.fetch2->num_tuples) {
            printf("SMALL1 FIRST BRANCH\n");
            printf("len1: %i\n", operator.fetch1->num_tuples);
            printf("len1: %i\n", operator.fetch2->num_tuples);
            
            _join(operator.store_as1, operator.store_as2, operator.fetch1, operator.fetch2, operator.sel1, 
                operator.sel2, operator.method, query->context->chandle_table, query->context->chandles_in_use);
      
        } else {
            printf("SMALL2 SECOND\n");
            printf("len1: %i\n", operator.fetch1->num_tuples);
            printf("len1: %i\n", operator.fetch2->num_tuples);
            _join(operator.store_as2, operator.store_as1, operator.fetch2, operator.fetch1, operator.sel2, 
                operator.sel1, operator.method, query->context->chandle_table, query->context->chandles_in_use);
            
        }
        query->context->chandles_in_use+=2;
        return "join";
    } else if (query && query->type == UPDATE) {
        UpdateOperator operator = query->operator_fields.update_operator;

        update(operator.column, operator.tbl, operator.result, operator.value);
        return "updte";
    } else if (query && query->type == DELETE) {
        DeleteOperator operator = query->operator_fields.delete_operator;
        printf("name of tbl in delete: %s\n", operator.tbl->name);
        printf("tbl lngth : %i\n", operator.tbl->table_length);
        delete_update(operator.tbl, operator.res);
        return "65";

    } else {    
        return "165";
    }
    printf("Segfault?\n");
    free(query);
    return "165";
}


/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/

void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = (ClientContext*) malloc(sizeof(ClientContext));
    client_context->chandle_table = (Result*) malloc(sizeof(Result) * 1000);
    //client_context->chandles_in_use = malloc(sizeof(int));
    client_context->chandles_in_use = 0;
    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    Table* table = NULL;
    int load_counter = 0;

    //int tracker = 0;
    int offset = 0;
    bool fl = false;
    do {
       // printf("reiterate\n");
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            printf("not done\n");
            done = 1;
        }
        if (recv_message.status==HERETABLE) {

            load_counter=-1;
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';
            printf("received from client: %s\n", recv_message.payload);
            table = &(current_db->tables[lookup_table(recv_message.payload)]);
            printf("Table name in server: %s\n", table->name);
            continue;
        } else if (recv_message.status == OK_LOAD || recv_message.status==OK_NEW_COL) {
            fl = true;
            if (recv_message.status == OK_NEW_COL) {
                printf("!!!!!incremented load counter\n");
                load_counter++;
                offset = 0;
            }
            int recv_buffer[recv_message.length/4];
           
           
            // printf("the length to receive is: %i\n", recv_message.length);
            
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload2 = recv_buffer;
            // printf("value 4 in recv: %i\n", recv_message.length);
            //recv_message.payload[recv_message.length] = '\0';
            load_columns(table, recv_buffer, load_counter, offset, recv_message.length);
           
            
            offset += (recv_message.length/4);

            //printf("value in my loaded column: %i\n", table->columns[0].data[1]);
            continue;
        } else if (recv_message.status == OK_BATCH_DONE) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            // while (current_db->num_in_batch > 0) {
            //     DbOperator* query = parse_command(current_db->query_batch[count_ex], &send_message, client_socket, client_context);
            //     char* result = execute_DbOperator(query, client_socket);
            //     printf("segfaul2\n");

            //     current_db->num_in_batch--;
            //     count_ex++;
            // }
         

            //fill_op_quer(current_db->query_batch, &send_message, client_socket, client_context);

            
            //printf("parse succesful line 470 server.c: %s\n", current_db->op_quer[0]->operator_fields.select_operator.table->name);
            dispatch_threads(client_socket, client_context);
            printf("donep2\n");
            wait_all();
            printf("donep3\n");
           // current_db->op_quer[0]->context->chandles_in_use += current_db->num_in_batch;
            client_context->chandles_in_use += current_db->num_in_batch;
            current_db->num_in_batch = 0;
            send_message.status = OK_DONE;
            send_message.length = 4;
            send_message.payload = "done";
            printf("line 481 server.c: %s\n",client_context->chandle_table[0].nm);
             if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message.");
                    exit(1);
                }

                // 4. Send response to the request
                if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                    log_err("Failed to send message.");
                    exit(1);
                }
            continue;
        }

        // if (recv_message.status != OK_LOAD && fl) {
        //     fl = false;
        //             for (int i = 0; i != current_db->tables_size; i++) {
        //         printf("inf: %i,%i\n", i,current_db->tables[i].index_create);
        //         for (int j = 0; j < current_db->tables[i].index_create;j++) {
        //             printf("index to be CREATED: %i,%i \n", i,j);
                    
        //             Column* col_for_ind = &current_db->tables[i].columns[find_column(current_db->tables[i].name, current_db->tables[i].col_names_to_index[j])];
        //             handle_index(col_for_ind, &current_db->tables[i], col_for_ind->type, col_for_ind->clustered);
        //         }
        //         current_db->tables[i].index_create = 0;
        //     }

        // }
        printf("line 473 server.c\n");
        if (!done) {
            
            // while (recv_message.status == HERETABLE || recv_message.status == OK_LOAD) {
                
              printf("recv\n");
            // }
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            if (recv_message.status == CLIENT_S) {
                current_db->partial_shutdown = true;
                shutdown_server();
                break;
            }

            if (recv_message.status == OK_BATCH) {
                printf("line 454 server.c ok_batch branch\n");
                //length = recv(client_socket, recv_buffer, recv_message.length,0);
                if (length > 0) {
                    printf("line 458 server.c: %s\n", current_db->name);
                    if(!current_db->query_batch) {
                        printf("but i just malloced line 460\n");
                    }
                    current_db->query_batch[current_db->num_in_batch] = malloc(strlen(recv_message.payload)+1);
                    printf("line 460 server.c\n");
                    strcpy(current_db->query_batch[current_db->num_in_batch], recv_message.payload); 
                    printf("line 462 server.c\n");
                    current_db->num_in_batch++;
                }

                // for (int i = 0; i != current_db->num_in_batch; i++) {
                //     printf("Name of column in batch: %s\n", current_db->query_batch[i]);
                // }
                
            }
            
            // 1. Parse command
            //    Query string is converted into a request for an database operator

            // if (global_result_count == 1) {
            // printf("client_cont: %s\n", client_context->chandle_table[0]->nm);
                
            // }

            //printf("This is the rec payload 417: %s\n", recv_message.payload);
            DbOperator* query = NULL;
           
           if (recv_message.status != OK_BATCH && recv_message.status != OK_BATCH_DONE){
                printf("The status is not ok_batch\n");
                query = parse_command(recv_message.payload, &send_message, client_socket, client_context);
            } 

            printf("line 412\n");
            // 2. Handle request
            //    Corresponding database operator is executed over the query
            char* result = execute_DbOperator(query, client_socket);
            printf("segfaul2\n");

            if (!result) {
                continue;
            }

            send_message.length = strlen(result);
            printf("segfault4\n");
            char send_buffer[send_message.length + 1];
            printf("segfault5\n");
            strcpy(send_buffer, result);
            printf("segfault3\n");
            send_buffer[send_message.length] = '\0';
            send_message.payload = send_buffer;

            
            if (recv_message.status == OK_BATCH) {
                printf("batch cont received\n");
                send_message.status = OK_BATCH;
            } else {
                send_message.status = OK_WAIT_FOR_RESPONSE;
            }

            if (recv_message.status != OK_LOAD && recv_message.status != OK_INT && recv_message.status != OK_BATCH_DONE && recv_message.status != CLIENT_S) {

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
                printf("sending response in server.c\n");
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    printf("sending response in server.c\n");
                    log_err("Failed to send message.");
                    exit(1);
                }

                // 4. Send response to the request
                if (send(client_socket, result, send_message.length, 0) == -1) {
                    printf("sending response in server.c\n");
                    log_err("Failed to send message.");
                    exit(1);
                }
                
                printf("response sent\n");

            if (strcmp(result, "shutdown") == 0) {
                printf("SHUTDOWN CALLED\n");
                close(client_socket);
            }
            }
            glob_mes_to_receive++;
        }
    } while (!done);
    printf("socket closed\n");

    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// function which will run a list of queries when starting up
void run_queries(char* list, int client_socket) {
    message send_message;
    ClientContext* client_context = malloc(sizeof(ClientContext));

    printf("line 509 client.c query: %s\n", list);
    DbOperator* query = parse_command(list, &send_message, client_socket, client_context);
    execute_DbOperator(query, client_socket);
    
    free(client_context);
}

void read_and_create(char* filename, int client_socket) {
    FILE* fp;
    char* line = NULL;
    size_t len = 0;
    ssize_t read;
    fp = fopen(filename, "r");

    int count_lines = 0;
    //int size_of_db = 0;
   // char* what = malloc(12);
    // count the number of lines in the file
    //printf("yuh101: %i\n", strlen(filename));
    while (count_lines != 1) {
        read = getline(&line, &len, fp);

         printf("Retrieved line of length %zu:\n", read);
         printf("%s\n", line);
         count_lines++;
    }
    printf("yuh222\n");
    int number_of_columns = 0;
    //message_status status = OK_DONE;
    //char* result = malloc(strlen(line));
    char* result = strsep(&line, ",");

    
    printf("yuh224: %s\n", result);
    char* db_name = strsep(&result, ".");
    printf("yuhy225\n");
    char* table_name = strsep(&result, ".");
    char** cols = malloc(strlen(result));
    cols[number_of_columns] = result;
    printf("yuh223: %s\n", result);
    int size_allocated = strlen(result);
    while ((result = strsep(&line, ",")) != NULL) {
        number_of_columns+=1;
        strsep(&result, ".");
        strsep(&result, ".");
        size_allocated += strlen(result);
        cols = realloc(cols, size_allocated);
        cols[number_of_columns] = result;
        printf("Result in read: %s\n", result);
    }
    number_of_columns+=1;

    char create_db_quer[13+strlen(db_name)];
    sprintf(create_db_quer, "create(db, %s)", db_name);
    printf("db_query: %s\n", create_db_quer);

    char create_tbl_quer[HANDLE_MAX_SIZE];
    sprintf(create_tbl_quer, "create(tbl, %s, %s, %i)", table_name, db_name, number_of_columns);
    printf("tbl_query: %s\n", create_tbl_quer);

    char** queries = malloc(strlen(create_tbl_quer) + strlen(create_tbl_quer));
    run_queries(create_db_quer, client_socket);
    run_queries(create_tbl_quer, client_socket);
    queries[0] = create_db_quer;
    queries[1] = create_tbl_quer;
    int size_of_queries = strlen(create_tbl_quer) + strlen(create_db_quer);
    for (int i = 0; i != number_of_columns; ++i) {
        char temp_buf[HANDLE_MAX_SIZE];
        sprintf(temp_buf, "create(col, %s, %s.%s)", cols[i], db_name, table_name);
        size_of_queries += strlen(temp_buf);
        //queries = realloc(queries, size_of_queries);
        run_queries(temp_buf, client_socket);
        //printf("query_col: %s\n", queries[i+2]);
    }

    // char load_query[HANDLE_MAX_SIZE];
    // sprintf(load_query, "load(%s)", filename);
    // printf("File name in server main: %s\n", filename);
    // run_queries(load_query, client_socket);
    //run_queries("print()", client_socket);
    
    fclose(fp);



}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
// 
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients? 
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void)
{
    while (true) {
        int server_socket = setup_server();
        if (server_socket < 0) {
            exit(1);
        }

        log_info("Waiting for a connection %d ...\n", server_socket);

        struct sockaddr_un remote;
        socklen_t t = sizeof(remote);
        int client_socket = 0;

        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }


            handle_client(client_socket);   
        
        //write(STDIN_FILENO, "HI", 2);
    }
    

    return 0;
}
