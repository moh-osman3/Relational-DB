/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#define _DEFAULT_SOURCE
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <math.h>

//#include <libexplain/socket.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "include/common.h"
#include "include/message.h"
#include "include/utils.h"
#include "cs165_api.h"
#include "client_context.h"


#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

char* next_per(char** token) {
    char* tokenn = strsep(token, ".");
    return tokenn;
}

char* get_table_name(char* token) {
  //  message_status status = OK_DONE;
  //  printf("line 59 client.c: %s\n", token);
    char** create_arguments_index = &token;
    char* tmp = strsep(create_arguments_index, ",");
    if (!tmp) {
        printf("dang\n");
    }

    //printf("line 62 client.c: \n");
    //printf("line 63 string: %s\n", );
    char* beg = strsep(create_arguments_index, ".");
    //printf("line 64 client.c\n");
    char* end = strsep(create_arguments_index, ".");
    //printf("line 66 client.c\n");
    char buf[strlen(beg)+strlen(end)+2];
    //printf("line 68 client.c\n");
    strcpy(buf, beg);
    //printf("line 70 client.c\n");
    strcpy(buf+strlen(beg), ".");
    strcpy(buf + strlen(beg) + 1, end);
    buf[strlen(beg) + strlen(end)+1] = '\0';
    char* bufferr = malloc(strlen(buf)+1);
    strcpy(bufferr, buf);
    //printf("line 81 client.c: %i\n", strlen(buf));
    return bufferr;
}

char dir[] = "/cs165/generated_data/";
//int dir_len = strlen(dir);
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

  //  log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        //fprintf(stderr, "%s\n", explain_socket(AF_UNIX, SOCK_STREAM, 0));
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    //log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}


char* read_and_create(char* filename, int num_tbls) {
    FILE* fp;
    char* line = NULL;
    size_t len = 0;
    //ssize_t read = 0;
    if (!filename) {
        return NULL;
    }
    fp = fopen(filename, "r");
    if (!fp) {
        return NULL;
    }
    //printf("read and create line 116\n");
    int count_lines = 0;
    //int size_of_db = 0;
   // char* what = malloc(12);
    // count the number of lines in the file
    //printf("yuh101: %i\n", strlen(filename));
    while (count_lines != 1) {
        getline(&line, &len, fp);

       // printf("Retrieved line of length %zu:\n", read);
       //printf("%s\n", line);
         count_lines++;
    }
    //printf("yuh222\n");
    int number_of_columns = 0;
    //message_status status = OK_DONE;
    //char* result = malloc(strlen(line));
    char* result = strsep(&line, ",");

   // printf("line 135 client.c\n");
    //printf("yuh224: %s\n", result);
    char* db_name = malloc(50);
    db_name = strsep(&result, ".");
   // printf("line 138 client.c: %s\n", db_name);
    //printf("yuhy225\n");
    char* table_name = malloc(50);
    table_name = strsep(&result, ".");

    // dont use realloc bro 
    // and add flag so dont recreate db1
    char** cols = malloc(sizeof(char*)*4);

    cols[number_of_columns] = malloc(500);
    (cols[number_of_columns])[0] = '\0';
    strcpy(cols[number_of_columns],result);
    //printf("line 150 client.c first column name: %s\n", cols[0]);
   // printf("line 142 client.c\n");
    //printf("yuh223: %s\n", result);
    int size_allocated = strlen(result);
   // int col_num = 0;
    while ((result = strsep(&line, ",")) != NULL) {
        number_of_columns+=1;
        strsep(&result, ".");
        strsep(&result, ".");
        size_allocated += strlen(result);
      //  printf("This is number_of_columns: %i\n", number_of_columns);
       //  printf("line 166 client.c first column name: %s\n", cols[0]);
        cols[number_of_columns] = malloc(500);
        strcpy(cols[number_of_columns],result);
      //  printf("Result in read: %s\n", result);
    }
    // printf("line 166 client.c first column name: %s\n", cols[0]);
    number_of_columns+=1;
   // printf("line154 client.c\n");
    //if (current_db->tables_size)
    char create_db_quer[13+strlen(db_name)];
    create_db_quer[0] = '\0';
    //char db_name2[strlen(db_name)+1];

    //db_name2[strlen(db_name)] = '\0';
   sprintf(create_db_quer, "create(db, %s)\n", db_name);
   // printf("db_query: %s::: %i\n", create_db_quer, jj);

    char create_tbl_quer[HANDLE_MAX_SIZE] = {0};
    //create_tbl_quer[0] = '\0';
    sprintf(create_tbl_quer, "create(tbl, %s, %s, %i)\n", table_name, db_name, number_of_columns);
    //printf("tbl_query: %s\n", create_tbl_quer);
    char queries[2000];
    if (num_tbls != 0) {
        create_db_quer[0] = '\0';
        
    }
   //  printf("line 187 client.c first column name: %s\n", cols[0]);
    //run_queries(create_db_quer, client_socket);
    //run_queries(create_tbl_quer, client_socket);
    queries[0] = '\0';
    strncpy(queries, create_db_quer, strlen(create_db_quer));
    //printf("Line 187 client.c: %i\n", strlen(create_db_quer));

    //printf("after one strcpy: %s", queries);
    strcpy(queries + strlen(create_db_quer), ";");
   // printf("line 192 client.c\n");
    strcpy(queries+strlen(create_db_quer) + 1, create_tbl_quer);
   // printf("line 194 client.c\n");
    // queries[0] = create_db_quer;
    // queries[1] = create_tbl_quer;
    int size_of_queries = strlen(create_tbl_quer) + strlen(create_db_quer)+1;
    strcpy(queries + size_of_queries, ";");
    size_of_queries++;
    //printf("line 200 client.c\n");
    for (int i = 0; i != number_of_columns; ++i) {
        char* temp_buf = malloc(HANDLE_MAX_SIZE + 50);
        temp_buf[0] = '\0';
    //    printf("Iter %i, line 203 \n", i);
     //   printf("This is the name of the column line 206: %s\n", cols[i]);
        sprintf(temp_buf, "create(col, %s, %s.%s)\n", cols[i], db_name, table_name);
       // printf("Column query: %s\n", temp_buf);
        strcpy(queries+size_of_queries, temp_buf);
        
        size_of_queries += strlen(temp_buf);


        strcpy(queries+size_of_queries, ";");
           
        
        size_of_queries++;
        //free(temp_buf);
        //queries = realloc(queries, size_of_queries);
        //run_queries(temp_buf, client_socket);
        //printf("query_col: %s\n", queries[i+2]);
    }
    //printf("line217 client.c\n");
    // char load_query[HANDLE_MAX_SIZE];
    // sprintf(load_query, "load(%s)", filename);
    // printf("File name in server main: %s\n", filename);
    // run_queries(load_query, client_socket);
    //run_queries("print()", client_socket);
    char load_query[HANDLE_MAX_SIZE];
    sprintf(load_query, "load(%s)", filename);
    strcpy(queries+size_of_queries, load_query);
    size_of_queries+=strlen(load_query);
    strcpy(queries+size_of_queries,";flag");
    fclose(fp);
//    queries[0] = 'c';
//    queries[1] = 'r';
    char* ret_val = malloc(size_of_queries+500);
   // printf("QUERIES IN CLIENT: %s", queries);
    strcpy(ret_val, queries);
    //printf("this is return: %s\n", queries);

    // for (int i = 0; i != number_of_columns; ++i) {
    //     free(cols[i]);
    // }
    free(cols);
    free(db_name);
    // free(table_name);
  //  printf("These are the queries line 237: %s\n", queries);
    return ret_val;


}

char* load_index() {
    FILE* fp = fopen("index.txt", "r");
    if (!fp) {
        return NULL;
    }
    char* line = NULL;
    size_t len = 0;
    getline(&line, &len, fp);
    //printf("length in load_ind: %i\n", len);
    // if (len == -1) {
    //     printf("empty file\n");
    //     return NULL;
    // }
    fclose(fp);
    //printf("LINE: %s\n", line);
    return line;

}
/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *      
**/
int main(void)
{
 //printf("uyes\n");
    //printf("y: %i", global_result_counter );
    int client_socket = connect_client();
//    printf("uyes\n");
    if (client_socket < 0) {
        exit(1);
    }

    FILE* fp;
    char * line = NULL;
    size_t len2 = 0;
    ssize_t read;

    //printf("gothere2\n");
    fp = fopen("catalogue.txt", "r");
    //printf("%i\n", RAND_MAX);
   
    //printf("go3\n");
    int size_of_db = 0;
    if (fp) {
        int count_lines = 0;
        
       // char* what = malloc(12);
        // count the number of lines in the file
        while (count_lines != 2) {
            read = getline(&line, &len2, fp);
            if (count_lines == 0) {
                size_of_db = atoi(line);
            }

      //       printf("Retrieved line of length %zu:\n", read);
        //     printf("%s", line);
             count_lines++;
             

        }
        //printf("donezo\n");
        fclose(fp);
       // message* stat;
        if (line) {
          //  printf("file to load: %s\n", line);
        }
    }
    //printf("go4\n");
    char* flname= NULL;
    //printf("mannnn0\n");

    if (line) {
      //  printf("mannn\n");
        flname= strsep(&line, ",");   
    }
    //printf("this is flname: \n");
   
    //printf("output: %s\n", out);

    message send_message;
    message recv_message;


    int index_load_flag = 0;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;
    bool flag = true;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
   
    //printf("client acting weird\n");
//    int dum_count = 0;
    int count99 = 0;
    bool batch_mode = false;
    char* out = read_and_create(flname,count99);
    while (flag || (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) ) {
      //  printf("hmm\n");
        // if (dum_count == 2) {
        //     flag = false;
        // }
        // dum_count++; 
        //printf("once ugin\n");

        if (size_of_db >= 0) { 
            if (count99 > 0) {
                //flname = strsep(&line, ",");
                //printf("yuh flname: %s\n");
                //out = read_and_create(flname);
            }
          
            if ((output_str == NULL) && flag && out) {
              //  printf("SIZE OF DBB: %i\n", size_of_db);

                char* comp_str = strsep(&out, ";");
             //  printf("THIS IS WHAT IS ABOUT TO BE READ BUFFER: %s\n", comp_str);
                if (strcmp(comp_str, "") != 0) {
                    strcpy(read_buffer, comp_str);
                } else {
                    continue;
                }
               
                if (strncmp(out, "flag",4) == 0) { 
                //printf("GETOUT!\n");
                    count99++;
                    size_of_db--;
                    flname = strsep(&line, ",");
              //  printf("yuh flname: %s\n", flname);
                //free(out);
                out = read_and_create(flname,count99);
                //index_load_flag++;
                //free(out);
                }

                 if (size_of_db==0 && index_load_flag == 0) {
                  //  printf("index exists to load\n");
                    out = load_index();
                    index_load_flag++;
                    //size_of_db--;
                }

               // printf("go execute\n");


                //break;
            }
           
        }

       // printf("READ BUFFER ABOUT TO BE SENT IN CLIENT: %s\n", read_buffer);
        // if (size_of_db > 0) {
        //     flag = true;
        // }
        //printf("what\n");
        if (!out) {
            //printf("out is done\n");
            // if (size_of_db == 1) {
            //     //printf("out is done2\n");
            //     size_of_db = 0;
            // }
            flag = false;
            //printf("HJDKLSFAHDPGHAPOGFA\n");
            // if (index_load_flag == 1) {

            //     flag = false;
   
            // } else {
            //     // out = load_index();
            //     // index_load_flag = 1;
            //     // if (out) {
            //     //     printf("JKDSLHFASDK\n");
            //     //     strcpy(read_buffer, out);
            //     // }
               
            //  flag = false;
            // }
        }
 
        //printf("hmm\n");
        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.

        send_message.length = strlen(read_buffer);
        if (send_message.length > 1 || flag) {
           // printf("This is the send mess payload line 401 client.c: %s\n", send_message.payload);
            if (strncmp(send_message.payload, "batch_e", 7) == 0) {
                send_message.status = OK_BATCH_DONE;
                batch_mode = false;
            } else if ((strncmp(send_message.payload, "batch_q", 7) == 0) || batch_mode) {
             //   printf("ENTERING BATCH MODE CLIENT\n");
                send_message.status = OK_BATCH;
                batch_mode = true;
            }else {
               // printf("NOT IN BATCH MODE CLIENT\n");
                 send_message.status = OK_DONE;
            }
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }
            //printf("Line 388 client.c printing send payload: %s\n", trim_quotes(trim_parenthesis(send_message.payload+4)));
            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }

          //  printf("line 425 client.c\n");
          //  printf("torecev, %i\n",  glob_mes_to_receive);
            //while (glob_mes_to_receive >= 0) {
            // Always wait for server response (even if it is just an OK message)
            int _done = 0;
           // printf("line 433 client.cc\n");
            while (!_done){ 
           //    printf("line 435\n");
            if ((len = recv(client_socket, &(recv_message), sizeof(message),0)) > 0) {
              //  printf("line 401\n");
              //  printf("line 433 client.c\n");
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];
                    _done = 1;
             //       printf("line 445 client.c\n");
                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
            //            printf("%s\n", payload);
                    }
              //      printf("line451\n");
                    if (strcmp(payload, "shutdown") == 0) {
 
                        close(client_socket);
                        return 0;
                    } 

                } else if (recv_message.status == OK_INT || recv_message.status == OK_DONEP || recv_message.status == OK_MULTPRINT) {
                     
                    int num_bytes = (int) recv_message.length;
                    int payload[num_bytes/4+1];


                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        //payload[num_bytes] = '\0';
                        for (int i = 0; i != (num_bytes/4); ++i) {
                            if (recv_message.status == OK_MULTPRINT) {
                                printf("%i,", payload[i]);
                            } else {
                                 printf("%i \n", payload[i]);     
                            }
                           
                            if (i == (num_bytes/4)-1 && recv_message.status == OK_DONEP) {
                               printf("\n");
    
                            }
                        }
                    }
           
                    glob_mes_to_receive--;
                } else if (recv_message.status == OK_LONG || recv_message.status == OK_LONG_MULTI) {
                    //printf("RECIEVED IN CLIENT\n");
                    int num_bytes = (int) recv_message.length;
                    //printf("num bytes to receive: %i\n", num_bytes);
                    long long payload[1];
                    //recv_message.status == OK_DONE;
                    //printf("entered client.c long\n");
                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        if (recv_message.status == OK_LONG) {
                           printf("%lli\n", payload[0]);
                        } else {
                            printf("%lli,", payload[0]);
                        }
                    }
                }


                else if (recv_message.status == OK_FLOAT || recv_message.status == OK_NEWLINE) {
                    //recv_message.status == OK_DONE;
                    int num_bytes = (int) recv_message.length;
                    double payload[num_bytes];


                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        //payload[num_bytes] = '\0';
                        //printf("NUM FLOAT BYTES RECIEVED: %i\n", num_bytes);
                        for (int i = 0; i != num_bytes/8; ++i) {
                            if (recv_message.status == OK_FLOAT) {
                                printf("%0.2lf,", payload[i]);     
                            } else {
                                printf("%0.2lf", payload[i]);
                            }
                           
                           if (recv_message.status == OK_NEWLINE) {
                                printf("\n");
                           }
                           
                        }
                    }
                } else if (recv_message.status == OK_BATCH) {
               //     printf("line 499 client.c\n");
                    _done = 1;
                    char payload[recv_message.length + 1];
                    if ((len = recv(client_socket, payload, recv_message.length, 0)) > 0) {
               //         printf("payload ok_batch: %s\n", payload);
                    }
                } else if (recv_message.status == OK_BATCH_DONE) {
         //           recv_message.status == OK_DONE;
                    _done = 1;
                }
                else if (recv_message.status == OK_LOAD) {
                    //printf("meh\n");
             //       printf("line517\n");
           //         recv_message.status == OK_DONE;
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];

                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
                  //      printf("%s\n", payload);
                    }
                    //char dir[] = "/cs165/generated_data/";
                    char* src = "";
                    int dir_len = strlen(src);
                    char buf[strlen(payload) + dir_len + 1];
                    strcpy(buf, src);
                    strcpy(buf + dir_len, payload);
                    buf[strlen(payload) + dir_len] = '\0';
                    FILE * fp;
                    char * line = NULL;
                    size_t len = 0;
                  //  ssize_t read;
                    char rel_ins2[19];
                    strcpy(rel_ins2, "relational_insert(");
                    rel_ins2[18] = '\0';
                    // char* rel_ins2 = "relational_insert(";
                    // 
              //      printf("line 543\n");
                //printf("meh2\n");
                    //strcat(dir,payload);


                    //printf("director: %s\n", payload);
                    fp = fopen(payload, "r");

                    if (!fp) {
             //           printf("line552\n");
                        return -1;
                    }
                    int count_lines = 0;
                    int size_of_db = 0;
                    int number_of_columns = 0;
                    // count the number of lines in the file and send the table name
                    while ((read = getline(&line, &len, fp)) != -1) {
                 //       printf("line 560\n");
                        if (count_lines == 0) {
              //              printf("line 562\n");
                           // printf("Line 448 client.c: %s\n", line);
                            char line_cop[strlen(line)+1];
                            strcpy(line_cop, line);
              //              printf("line 566: %s\n", line_cop);
                            char* tmp = get_table_name(line_cop);
             //               printf("line 568: %s\n", tmp);
                            size_of_db = strlen(tmp);
                            while (line) {
                              //  printf("Line 448 client.c: %s\n", line);
                                number_of_columns += 1;
                  //              printf("line 573\n");
                                strsep(&line, ",");
                            }
                 //           printf("line 573\n");
                            char tbl_name[size_of_db+1];
                            strcpy(tbl_name, tmp);
                            tbl_name[size_of_db] = '\0';
                            // send table name to socket
                            char* tbl_need = tbl_name;
                            strsep(&tbl_need, ".");
                            message send_table;
                            send_table.status = HERETABLE;
                            send_table.payload = tbl_need;
                            send_table.length = strlen(tbl_need);
                 //         printf("THIS IS THE TABLE NAME IN CLIENT: %s\n", tbl_need);
                            if (send(client_socket, &(send_table), sizeof(message), 0) == -1) {
                                return -1;
                            }
                            //printf("yuh99: %i\n", strlen(rel_ins_statement));

                            if (send(client_socket, send_table.payload, send_table.length, 0) == -1) {
                                return -1;
                            }

                        }
                      //   printf("Retrieved line of length %zu:\n", read);
                        // printf("%s", line);
                         //printf("is it being incremented?\n");
                         count_lines++;
                    }

                    fclose(fp);
                    int** columns_to_send = malloc(sizeof(int*)*number_of_columns);
                    //printf("value of count_lines in client.c: %i\n", number_of_columns);
                    for (int i = 0; i != number_of_columns; ++i) {
                        columns_to_send[i] = malloc(sizeof(int)*count_lines);
                    }
                    // if (fp == NULL)
                    // //   exit(EXIT_FAILURE);
  //                  char rel_ins[strlen(rel_ins2) + size_of_db + 2];
                   // rel_ins[0] = '\0';
                    //printf("this is the payload: %s\n", payload);
                    fp = fopen(payload, "r");

                    if (!fp) {
                        return -1;
                    }
                    int skip_first_line = 0;
                    //int number_of_columns = 1;
                    int count_where = 0;

                   // printf("client opening file second time to send\n");
                    while ((read = getline(&line, &len, fp)) != -1) {
                
                         
                       // printf("client entered file open while loop 2\n");
                         if (skip_first_line > 0) {
                         //   int nums_to_send[number_of_columns];
                            int k = 0;
                            while (line) {
                             //   printf("Line 540 of client.c\n");
                                //printf("This is the value of k in client: %i\n", count_where);
                                (columns_to_send[k])[count_where] = atoi(strsep(&line, ","));
                               // printf("This is line: %s\n", line);
                                //nums_to_send[k] = atoi(strsep(&line, ","));
                               // printf("num:%i\n", (columns_to_send[k])[count_where]);
                               // printf("This is the value of k in client: %i\n", k);
                                k++;
                            }
                            //printf("\n");
                            
                           // printf("Line 551 client.c\n");
                            //char send_buffer[send_message2.length + 1];
                            //memcpy(send_buffer, result[res].payload, 4*result[res].num_tuples);
                            // char rel_ins_statement[strlen(rel_ins) + read + 2];
                            // strcpy(rel_ins_statement, rel_ins);
                            // strcpy(rel_ins_statement+strlen(rel_ins), line);
                            // rel_ins_statement[strlen(rel_ins)+read] = ')';
                           // rel_ins_statement[strlen(rel_ins)+read+1] = '\0';                   
                           // printf("send_buffer: %s\n", rel_ins);
                            count_where++; 
                         }
                        // printf("Line 562 client.c\n");
                         skip_first_line++;
                         
                     }

                    // for (int i = 0; i != 1000; ++i) {
                    //     printf("This is the val of column in client: %i\n", (columns_to_send[0])[i]);
                    // }
                    //printf("COMPLETE SENT \n");
                    fclose(fp);
                   // printf("Line 572 client.c\n");
                        for (int i = 0; i != number_of_columns; ++i) {
                            int total_to_send = 4*count_lines-1;
                            int* tmp_buffer = columns_to_send[i];
                           // printf("value of i in client: %i\n", i);
                            int counter101 = 0;
                          //  printf("line 578 client.c\n");
                            while (total_to_send > 0) {
                               // printf("total_to_send: %i", total_to_send);
                                message send_message2;
              
                                if (counter101 == 0) {
                                    counter101++;
                                    send_message2.status = OK_NEW_COL;
                                   // printf("the number of times i see the OK_NEW_COL: %i", i);
                                } else {
                                    send_message2.status = OK_LOAD;    
                                }

                                
                                
                                   
                                if (total_to_send > 1024) {

                                    send_message2.length = 1024;   
                                } else {
                                    send_message2.length = total_to_send;
                                }

                                int send_bf[send_message2.length];

                                memcpy(send_bf, tmp_buffer, send_message2.length);

                                send_message2.payload2 = send_bf;


                                if (send(client_socket, &(send_message2), sizeof(message), 0) == -1) {
                                    return -1;
                                }
                                //printf("yuh99: %i\n", strlen(rel_ins_statement));

                                if (send(client_socket, send_message2.payload2, send_message2.length, 0) == -1) {
                                    return -1;
                                }
                                total_to_send-=send_message2.length;
                                tmp_buffer += ((send_message2.length / 4));
                            }
                        }
                    }
                    glob_mes_to_receive--;
                   // current_db->wt = 1;
                }
                 
         //   }
            }
        }
          //   else {
          //       if (len < 0) {
          //           log_err("Failed to receive message.");
          //       }
          //       else {
		        //     log_info("-- Server closed connection\n");
		        // }
          //       exit(1);
          //   }
        
    }
  //  printf("line 775\n");
    message send_message_final;
    strcpy(send_message_final.payload, "c_shutdown");
    send_message_final.status = CLIENT_S;
    send_message_final.length = 10;
    //log_info("Connection closed at socket %d!\n", client_socket);
    if (send(client_socket, &(send_message_final), sizeof(message), 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
    }

    // 4. Send response to the request
    if (send(client_socket, send_message_final.payload, send_message_final.length, 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
    }
    close(client_socket);
    return 0;
}
