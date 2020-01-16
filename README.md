## Relational Database

To read about my implementation and design decisions please see final_report.pdf

To run my code:

Go to src file and run "make", then run ./server in one terminal and ./client in another terminal.  Type your commands in the client terminal.

## Functions Supported

### Create 

create(db,"db1")
create(tbl,"tbl1",db1,2)
create(col,"col1",db1.tbl1)
create(idx,db1.tbl1.col2,btree,unclustered)

### Load

load("filename.csv")

### Shutdown

shutdown

### Insert

relational_insert(db1.tbl1,-1,-11)

### Select

s1=select(db1.tbl1.col1,-4554,3446)

### Fetch

f1=fetch(db1.tbl2.col3,s1)


### Sum, Min, Max, Avg, Add, Sub

a1=sum(f1)
a2=min(f1)
a3=max(f1)
a4=avg(f1)
a5=add(f1,f1)
a6=sub(f1,f1)


### Batch Queries

batch_queries()
s0=select(db1.tbl3_batch.col2,2968,2988)
s1=select(db1.tbl3_batch.col2,9923,9943)
s2=select(db1.tbl3_batch.col2,8660,8680)
batch_execute()


### Joins (Nested and hash join)

p1=select(db1.tbl5_fact.col2,null, 1600)
p2=select(db1.tbl5_dim2.col1,null, 6400)
f1=fetch(db1.tbl5_fact.col4,p1)
f2=fetch(db1.tbl5_dim2.col1,p2)
t1,t2=join(f1,p1,f2,p2,hash)

credit to cs165 @ Harvard for basic starter api
