### This is a test dummy example of running a couple generated tests ###

#### TO BE RUN INSIDE DOCKER #####
# run from base dirctory as ./infra_scripts/test.sh

# the test directory, containing generated data dl, exp, and csvs
# expressed, relative to the base project directory
REL_TEST_DIR=generated_data
# absolute with the docker mount points
ABS_TEST_DIR=/cs165/$REL_TEST_DIR

STUDENT_OUTPUT_DIR=/cs165/student_outputs

DATA_SIZE=100000 
RAND_SEED=41

# create milestone 1 data
cd project_tests/data_generation_scripts
python milestone1.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR
 python milestone2.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR
# python milestone3.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR
# python milestone4.py 10000 8000 8000 13 1.0 50  $ABS_TEST_DIR $ABS_TEST_DIR
#python milestone5.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR


# setup code
cd ../../src
make clean
make all

# record results of tests
# echo ""
# echo "running test 1"
# ./server > $STUDENT_OUTPUT_DIR/test01gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test01gen.dsl
# echo ""
# echo "running test 2"
# ./server > $STUDENT_OUTPUT_DIR/test02gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test02gen.dsl
# echo ""
# echo "running test 3"
# time ./server > $STUDENT_OUTPUT_DIR/test03gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test03gen.dsl
# echo ""
# echo "running test 4"
# ./server > $STUDENT_OUTPUT_DIR/test04gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test04gen.dsl
# echo ""
# echo "running test 5"
# ./server > $STUDENT_OUTPUT_DIR/test05gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test05gen.dsl
# echo ""
# echo "running test 6"
# ./server > $STUDENT_OUTPUT_DIR/test06gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test06gen.dsl
# echo ""
# echo "running test 7"
# ./server > $STUDENT_OUTPUT_DIR/test07gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test07gen.dsl
# echo ""
# echo "running test 8"
# ./server > $STUDENT_OUTPUT_DIR/test08gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test08gen.dsl
# echo ""
# echo "running test 9"
# ./server > $STUDENT_OUTPUT_DIR/test09gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test09gen.dsl
# echo ""
# echo "running test 10"
# ./server > $STUDENT_OUTPUT_DIR/test10gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test10gen.dsl
# echo ""
# echo "running test 11"
# ./server > $STUDENT_OUTPUT_DIR/test11gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test11gen.dsl
# echo ""
# echo "running test 12"
# ./server > $STUDENT_OUTPUT_DIR/test12gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test12gen.dsl
# echo ""
# echo "running test 13"
# ./server > $STUDENT_OUTPUT_DIR/test13gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test13gen.dsl
# echo ""
# echo "running test 14"
# ./server > $STUDENT_OUTPUT_DIR/test14gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test14gen.dsl
# echo ""
# echo "running test 15"
# ./server > $STUDENT_OUTPUT_DIR/test15gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test15gen.dsl
# echo ""
# echo "running test 16"
# ./server > $STUDENT_OUTPUT_DIR/test16gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test16gen.dsl
# echo ""
# echo "running test 17"
# ./server > $STUDENT_OUTPUT_DIR/test17gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test17gen.dsl
# echo ""
# echo "running test 18"
# ./server > $STUDENT_OUTPUT_DIR/test18gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test18gen.dsl
# echo ""
# echo "running test 19"
# ./server > $STUDENT_OUTPUT_DIR/test19gen.server.debug.out &
# sleep 1
# valgrind --tool=cachegrind ./client < ../$REL_TEST_DIR/test19gen.dsl
# echo ""
# echo "running test 20"
# ./server > $STUDENT_OUTPUT_DIR/test20gen.server.debug.out &
# sleep 1
# valgrind --tool=cachegrind ./client < ../$REL_TEST_DIR/test20gen.dsl
# echo ""
# echo "running test 21"
# ./server > $STUDENT_OUTPUT_DIR/test21gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test21gen.dsl
# echo ""
# echo "running test 22"
# ./server > $STUDENT_OUTPUT_DIR/test22gen.server.debug.out &
# sleep 1
# valgrind --tool=cachegrind ./client < ../$REL_TEST_DIR/test22gen.dsl
# echo ""
# echo "running test 23"
# ./server > $STUDENT_OUTPUT_DIR/test23gen.server.debug.out &
# sleep 1
# valgrind --tool=cachegrind ./client < ../$REL_TEST_DIR/test23gen.dsl
# echo ""
# echo "running test 24"
# ./server > $STUDENT_OUTPUT_DIR/test24gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test24gen.dsl
# echo ""
# echo "running test 25"
# ./server > $STUDENT_OUTPUT_DIR/test25gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test25gen.dsl
# echo ""
# echo "running test 26"
# ./server > $STUDENT_OUTPUT_DIR/test26gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test26gen.dsl
# echo ""
# echo "running test 27"
# ./server > $STUDENT_OUTPUT_DIR/test27gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test27gen.dsl
# echo ""
# echo "running test 28"
# ./server > $STUDENT_OUTPUT_DIR/test28gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test28gen.dsl
# echo ""
# echo "running test 29"
# ./server > $STUDENT_OUTPUT_DIR/test29gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test29gen.dsl
# echo ""
# echo "running test 30"
# ./server > $STUDENT_OUTPUT_DIR/test30gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test30gen.dsl
# echo ""
# echo "running test 31"
# ./server > $STUDENT_OUTPUT_DIR/test31gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test31gen.dsl
# echo ""
# echo "running test 32"
# ./server > $STUDENT_OUTPUT_DIR/test32gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test32gen.dsl
# echo ""
# echo "running test 33"
# ./server > $STUDENT_OUTPUT_DIR/test33gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test33gen.dsl
# echo ""
# echo "running test 34"
# ./server > $STUDENT_OUTPUT_DIR/test34gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test34gen.dsl
echo ""
echo "running test 35"
./server > $STUDENT_OUTPUT_DIR/test35gen.server.debug.out &
sleep 1
time ./client < ../$REL_TEST_DIR/test35gen.dsl
echo ""
echo "running test 36"
./server > $STUDENT_OUTPUT_DIR/test36gen.server.debug.out &
sleep 1
time ./client < ../$REL_TEST_DIR/test36gen.dsl 
 
# echo ""
# echo "running test 37"
# ./server > $STUDENT_OUTPUT_DIR/test37gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test37gen.dsl 

# echo "running test 38"
# ./server > $STUDENT_OUTPUT_DIR/test38gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test38gen.dsl 
 
# echo ""
# echo "running test 39"
# ./server > $STUDENT_OUTPUT_DIR/test39gen.server.debug.out &
# sleep 1
# ./client < ../$REL_TEST_DIR/test39gen.dsl > test39.out

# echo "running test 40"
# ./server > $STUDENT_OUTPUT_DIR/test40gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test40gen.dsl 
 
# echo ""
# echo "running test 41"
# ./server > $STUDENT_OUTPUT_DIR/test41gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test41gen.dsl > test41.out

# echo "running test 42"
# ./server > $STUDENT_OUTPUT_DIR/test42gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test42gen.dsl > test42.out
 
# echo ""
# echo "running test 43"
# ./server > $STUDENT_OUTPUT_DIR/test43gen.server.debug.out &
# sleep 1
# time ./client < ../$REL_TEST_DIR/test43gen.dsl > test43.out


if pgrep server; then pkill server; fi
