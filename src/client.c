/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
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


char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

// finds next string after periods
char* next_per(char** token) {
    char* tokenn = strsep(token, ".");
    return tokenn;
}

// gets table name from db.tbl.col
char* get_table_name(char* token) {
  
    char** create_arguments_index = &token;
    char* tmp = strsep(create_arguments_index, ",");
    

 
    char* beg = strsep(create_arguments_index, ".");
 
    char* end = strsep(create_arguments_index, ".");
    
    // copy table name
    char buf[strlen(beg)+strlen(end)+2];
 
    strcpy(buf, beg);
 
    strcpy(buf+strlen(beg), ".");
    strcpy(buf + strlen(beg) + 1, end);
    buf[strlen(beg) + strlen(end)+1] = '\0';
    char* bufferr = malloc(strlen(buf)+1);

    if (!bufferr) {
        return NULL;
    }
    strcpy(bufferr, buf);
 
    return bufferr;
}

char dir[] = "/cs165/generated_data/";
 
/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/

int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

 

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        
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


/* 
* This function takes a file name and generates the queries
* needed to create necessary tables and columns
*/
char* read_and_create(char* filename, int num_tbls) {
    FILE* fp;
    char* line = NULL;
    size_t len = 0;
    //ssize_t read = 0;
    if (!filename) {
        return NULL;
    }

    // open csv file to load
    fp = fopen(filename, "r");
    if (!fp) {
        return NULL;
    }

    int count_lines = 0;
 
    // count the number of lines in the file
    while (count_lines != 1) {
        getline(&line, &len, fp);
        count_lines++;
    }
 
    int number_of_columns = 0;
 
    char* result = strsep(&line, ",");

 
    char* db_name = malloc(50);

    if (!db_name) {
        return NULL;
    }

    db_name = strsep(&result, ".");
 
    char* table_name = malloc(50);
    table_name = strsep(&result, ".");

    // allocate space to read in the data values
    char** cols = malloc(sizeof(char*)*4);
    if (!cols) {
        return NULL;
    }
    cols[number_of_columns] = malloc(500);
    (cols[number_of_columns])[0] = '\0';
    strcpy(cols[number_of_columns],result);
    int size_allocated = strlen(result);
   
   // fill columns with dara
    while ((result = strsep(&line, ",")) != NULL) {
        number_of_columns+=1;
        strsep(&result, ".");
        strsep(&result, ".");
        size_allocated += strlen(result);
        cols[number_of_columns] = malloc(500);
        strcpy(cols[number_of_columns],result);
      //  printf("Result in read: %s\n", result);
    }
 
    number_of_columns+=1;
 
    char create_db_quer[13+strlen(db_name)];
    create_db_quer[0] = '\0';
 
    sprintf(create_db_quer, "create(db, %s)\n", db_name);
    

    char create_tbl_quer[HANDLE_MAX_SIZE] = {0};
 
    sprintf(create_tbl_quer, "create(tbl, %s, %s, %i)\n", table_name, db_name, number_of_columns);
 
    char queries[2000];
    if (num_tbls != 0) {
        create_db_quer[0] = '\0';
        
    }
 
    queries[0] = '\0';
    strncpy(queries, create_db_quer, strlen(create_db_quer));
 
    strcpy(queries + strlen(create_db_quer), ";");
 
    strcpy(queries+strlen(create_db_quer) + 1, create_tbl_quer);
 
    int size_of_queries = strlen(create_tbl_quer) + strlen(create_db_quer)+1;
    strcpy(queries + size_of_queries, ";");
    size_of_queries++;
 
    // create columns queries
    for (int i = 0; i != number_of_columns; ++i) {
        char* temp_buf = malloc(HANDLE_MAX_SIZE + 50);
        temp_buf[0] = '\0';
 
        sprintf(temp_buf, "create(col, %s, %s.%s)\n", cols[i], db_name, table_name);
    
        strcpy(queries+size_of_queries, temp_buf);
        
        size_of_queries += strlen(temp_buf);


        strcpy(queries+size_of_queries, ";");
           
        
        size_of_queries++;
 
    }
 
    char load_query[HANDLE_MAX_SIZE];
    sprintf(load_query, "load(%s)", filename);
    strcpy(queries+size_of_queries, load_query);
    size_of_queries+=strlen(load_query);
    strcpy(queries+size_of_queries,";flag");
    fclose(fp);
 
    char* ret_val = malloc(size_of_queries+500);

    if (!ret_val) {
        return NULL;
    }
 
    strcpy(ret_val, queries);
 
 
    free(cols);
    free(db_name);
 
    return ret_val;

}


// this function is to persist indexes created by previous client
char* load_index() {
    FILE* fp = fopen("index.txt", "r");
    if (!fp) {
        return NULL;
    }
    char* line = NULL;
    size_t len = 0;
    getline(&line, &len, fp);
 
    fclose(fp);
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
 
    int client_socket = connect_client();
 
    if (client_socket < 0) {
        exit(1);
    }

    FILE* fp;
    char * line = NULL;
    size_t len2 = 0;
    ssize_t read;

    // this is my file catologue which keeps track of tables and columns
    fp = fopen("catalogue.txt", "r");
    int size_of_db = 0;
    if (fp) {
        int count_lines = 0;
        
        // count the number of lines in the file
        while (count_lines != 2) {
            read = getline(&line, &len2, fp);
            if (count_lines == 0) {
                size_of_db = atoi(line);
            }

            count_lines++;
            
        }
        fclose(fp);
      
    }
    char* flname= NULL;
    
    if (line) {
        flname= strsep(&line, ",");   
    }
    
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
    bool flag = true; // flag for persistance (loads in files)

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
   

    int count99 = 0;


    bool batch_mode = false; // flag to signal batch queries
    char* out = read_and_create(flname,count99);
    while (flag || (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) ) {
 
        // check that db and tables exist in catologue
        if (size_of_db >= 0) { 
          
          
            if ((output_str == NULL) && flag && out) {
          

                char* comp_str = strsep(&out, ";");
 
                if (strcmp(comp_str, "") != 0) {
                    strcpy(read_buffer, comp_str);
                } else {
                    continue;
                }
               
                if (strncmp(out, "flag",4) == 0) { 
   
                    count99++;
                    size_of_db--;
                    flname = strsep(&line, ",");
        
                out = read_and_create(flname,count99);
 
                }

                // check if there are indexes to load
                 if (size_of_db==0 && index_load_flag == 0) {
  
                    out = load_index();
                    index_load_flag++;
        
                }

            
            }
           
        }

        // stop looping once catalogue data is loaded
        if (!out) {
            flag = false;
        }
 
        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.

        send_message.length = strlen(read_buffer);
        if (send_message.length > 1 || flag) {
            // these first couple of if statements set proper flags to signal to server
            // in the case where batch querying is called
            if (strncmp(send_message.payload, "batch_e", 7) == 0) {
                send_message.status = OK_BATCH_DONE;
                batch_mode = false;
            } else if ((strncmp(send_message.payload, "batch_q", 7) == 0) || batch_mode) {
                send_message.status = OK_BATCH;
                batch_mode = true;
            }else {
                 send_message.status = OK_DONE;
            }
            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }
            // Send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }

            // Always wait for server response (even if it is just an OK message)
            int _done = 0;
            // loop through to send info to server
            while (!_done){ 
            if ((len = recv(client_socket, &(recv_message), sizeof(message),0)) > 0) {
                if ((recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) &&
                    (int) recv_message.length > 0) {
                    // Calculate number of bytes in response package
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];
                    _done = 1;
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
                    }
                    if (strcmp(payload, "shutdown") == 0) {
 
                        close(client_socket);
                        return 0;
                    } 

                } else if (recv_message.status == OK_INT || recv_message.status == OK_DONEP || recv_message.status == OK_MULTPRINT) {
                     
                    int num_bytes = (int) recv_message.length;
                    int payload[num_bytes/4+1];


                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                         
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
            
                    int num_bytes = (int) recv_message.length;
           
                    long long payload[1];
           
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
         
                    int num_bytes = (int) recv_message.length;
                    double payload[num_bytes];


                    // Receive the payload and print it out
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
            
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
 
                    _done = 1;
                    char payload[recv_message.length + 1];
                    if ((len = recv(client_socket, payload, recv_message.length, 0)) > 0) {
       
                    }
                } else if (recv_message.status == OK_BATCH_DONE) {
 
                    _done = 1;
                }
                else if (recv_message.status == OK_LOAD) {
 
                    int num_bytes = (int) recv_message.length;
                    char payload[num_bytes + 1];

                    // Receive the payload  
                    if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                        payload[num_bytes] = '\0';
 
                    }
                    
                    char* src = "";
                    int dir_len = strlen(src);
                    char buf[strlen(payload) + dir_len + 1];
                    strcpy(buf, src);
                    strcpy(buf + dir_len, payload);
                    buf[strlen(payload) + dir_len] = '\0';
                    FILE * fp;
                    char * line = NULL;
                    size_t len = 0;
            
                    char rel_ins2[19];
                    strcpy(rel_ins2, "relational_insert(");
                    rel_ins2[18] = '\0';
 

                    fp = fopen(payload, "r");

                    if (!fp) {
 
                        return -1;
                    }
                    int count_lines = 0;
                    int size_of_db = 0;
                    int number_of_columns = 0;

                    // count the number of lines in the file and send the table name
                    while ((read = getline(&line, &len, fp)) != -1) {
 
                        if (count_lines == 0) {
 
                            char line_cop[strlen(line)+1];
                            strcpy(line_cop, line);
 
                            char* tmp = get_table_name(line_cop);
 
                            size_of_db = strlen(tmp);
                            while (line) {
 
                                number_of_columns += 1;
 
                                strsep(&line, ",");
                            }
  
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
   
                            if (send(client_socket, &(send_table), sizeof(message), 0) == -1) {
                                return -1;
                            }
        

                            if (send(client_socket, send_table.payload, send_table.length, 0) == -1) {
                                return -1;
                            }

                        }
             
                         count_lines++;
                    }

                    fclose(fp);

                    // allocate space for the data that will be loaded
                    int** columns_to_send = malloc(sizeof(int*)*number_of_columns);
                    
                    if (!columns_to_send){
                        return -1;
                    }
                    for (int i = 0; i != number_of_columns; ++i) {
                        columns_to_send[i] = malloc(sizeof(int)*count_lines);
                    }
 
                    fp = fopen(payload, "r");

                    if (!fp) {
                        return -1;
                    }
                    int skip_first_line = 0;
          
                    int count_where = 0;

                    while ((read = getline(&line, &len, fp)) != -1) {
                
                         
      
                         if (skip_first_line > 0) {

                            int k = 0;
                            while (line) {
                                // sending ascii text over the socket slows down my load
                                // function so sending integers over socket instead
                                (columns_to_send[k])[count_where] = atoi(strsep(&line, ","));
               
                                k++;
                            }
                  
                            count_where++; 
                         }
        
                         skip_first_line++;
                         
                     }
 
                    fclose(fp);
                        
                        for (int i = 0; i != number_of_columns; ++i) {
                            int total_to_send = 4*count_lines-1;
                            int* tmp_buffer = columns_to_send[i];
                       
                            int counter101 = 0;
                      
                            // keep looping until theres no more data to send to server
                            while (total_to_send > 0) {
                  
                                message send_message2;
              
                                if (counter101 == 0) {
                                    counter101++;
                                    send_message2.status = OK_NEW_COL;
                           
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
