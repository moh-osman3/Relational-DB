

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H
#define PAGESIZE 4000
#define INSERTPOOLSZ 1000
//#define PAGESIZE TPAGESIZE/2
#define SIZEOFRES 130000000

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/


typedef struct ClusterTuple {
    int val;
    int id;
} ClusterTuple;

typedef enum DataType {
     INT,
     LONG,
     FLOAT
} DataType;

typedef enum StoreType {
    BTREE,
    SORTED,
    NOIDX
} StoreType;

  
struct Comparator;
//struct ColumnIndex;


typedef struct BtreeNode {
  bool leaf; // true if node is leaf
  bool last_node;
  int data[PAGESIZE/4];
  int num_elt;
  int start_id;
  struct BtreeNode* child_block;
  struct BtreeNode* next;
  struct BtreeNode* prev;
  int orig_ids[PAGESIZE/4];
} BtreeNode;


typedef union ColumnIndex {
  ClusterTuple* sorted;
  BtreeNode* data_tree;
   
} ColumnIndex;
  
// typedef struct IndexInsertPool {
//     int vals_to_add[INSERTPOOLSZ];
//     int num_in_pool;
// } IndexInsertPool;

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    int* data;
    // You will implement column indexes later. 
    //void* index;
    StoreType type;
    int vals_to_add[INSERTPOOLSZ];
    ColumnIndex* index;
    bool clustered;
    int num_in_pool;    
} Column;


typedef enum SelectType {
    BIT,
    RANGE,
    IDX,
    IDX2,
    FTH,
    OP
} SelectType;

typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    char nm[MAX_SIZE_NAME];
    int payload[SIZEOFRES];
    double payload_float[1];
    long long payload_long[1];
    int orig_lngth;
    SelectType select_type;
} Result;

/**
 * table
 * efines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    //int num_in_pool; //stores the number of inserts needed for update
    size_t col_count;
    size_t counter;
    size_t table_length;
    Column* cluster_copy;
    int num_idx;
    char** col_names_to_index;

    // delete variable pool
    int index_create; // flag if an index needs to be created

    int num_dels;
    Result* chandle_deletes;


} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
**/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
    char** query_batch;
    int num_in_batch;
    bool unclust_exists;
    bool clust_exists;
    bool partial_shutdown;
    int wt;
    bool off;

} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated n
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;



// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*q
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */




/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
    GeneralizedColumnType type;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    Result* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;



/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    SELECT,
    FETCH,
    PRINT,
    LOAD,
    ARITH,
    SHUTDOWN, 
    BINOP, 
    BATCHQ,
    INDEX,
    JOIN,
    UPDATE,
    DELETE,
} OperatorType;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator {
    CreateType create_type; 
    char name[MAX_SIZE_NAME]; 
    Db* db;
    Table* table;
    int col_count;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;

    int* values;
} InsertOperator;

typedef struct SelectOperator {
    char* previous_select;
    Table* table;
    Column* column;
    char* interm;
    Result* res_for_range;
    Result* res_bitvector;
    int lower;
    int upper;

} SelectOperator;
/*
 * necessary fields for insertion
 */
typedef struct FetchOperator {
    Table* table;
    Column* column;
    char* res_name;
    char* interm;
} FetchOperator;

typedef struct PrintOperator {
    char** names_to_load;
    int num_to_print;
} PrintOperator;

typedef struct LoadOperator {
    char* file_name;
} LoadOperator;

typedef struct ArithOperator {
    char* col_name;
    char* store_as;
    char op[4];
} ArithOperator;

typedef struct BinopOperator {
    char* col1_name;
    char* col2_name;
    char* store_as;
    char* op[4];
} BinopOperator;

typedef struct IndexOperator {
    Column* column_to_index;
    Table* tbl;
    bool is_clustered;
    StoreType type;
  
} IndexOperator;

typedef struct JoinOperator {
    char* store_as1;
    char* store_as2;
    Result* fetch1;
    Result* fetch2;
    Result* sel2;
    Result* sel1;
    char* method;
} JoinOperator;

typedef struct UpdateOperator {
    Table* tbl;
    Column* column;
    Result* result;
    int value;

} UpdateOperator;


typedef struct DeleteOperator {
    Table* tbl;
    Result* res;
} DeleteOperator;


/*

 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    LoadOperator load_operator;
    ArithOperator arith_operator;
    BinopOperator binop_operator;
    IndexOperator index_operator;
    JoinOperator join_operator;
    UpdateOperator update_operator;
    DeleteOperator delete_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

typedef struct thread_in {
    int num_threads;
    int thread_id;
    int num_to_execute;
    ClientContext* context;
    int client_socket;
} thread_in;

extern Db *current_db;
 
/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

Status create_db(const char* db_name);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

Status create_column(char *name, Table *table);

void shutdown_server();
void do_binop(int ind_needed, char* operation, char* store_as, 
                Result* res1, Result* res2, Result* result_list);
void do_arith(int ind_needed, char* operation, char* store_as , Result* column_for_op, Result* result_list);
void fetch(Result* res, int ind_needed, Table* table, Column* column, char* interm, Result* intermediate);
Status relational_insert(Table* table, int* values);
Result* selectt(Result* res, int index_needed, Table* table, Column* column, char* interm, int lower, int upper);

void _select2(Result* res_bitvector, Result* res_for_range, Result* res, 
                int num_results, char* name, int lower, int upper);

Status relational_insert(Table* table, int* values);
char** execute_db_operator(DbOperator* query, int client_sock);
void db_operator_free(DbOperator* query);
void dispatch_threads(int client_socket, ClientContext* context);
void wait_all();
Status load_columns(Table* table, int* vals, int index, int offset, int size);
void handle_index(Column* col, Table* tbl, StoreType type, bool is_clustered);
//int _max(Result* result, int* res);
void cluster_fetch(Result* res, int ind_needed, Table* table, Column* column, char* interm, Result* intermediate);
void clustered_select(Result* res, int index_needed, Table* table, Column* column, char* interm, int lower, int upper);
void unclust_select(Result* res, int index_needed, Table* table, Column* column, char* interm, int lower, int upper);
void id_fetch(Result* res, int ind_needed, Column* column, Table* table, char* interm, Result* intermediate);
void _join(char* store1, char* store2, Result* fetch1, Result* fetch2, Result* sel1,
     Result* sel2, char* method, Result* chand_tbl, int num_elt);
void insert_index(Table* table, Column* col, int* vals);
void update(Column* column, Table* table, Result* result, int value);
void delete_update(Table* tbl, Result* res);
#endif /* CS165_H */

