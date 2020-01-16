 
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
RAND_SEED=42

# create milestone 1 data
cd project_tests/data_generation_scripts
python milestone2.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR
python milestone3.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR
python exp1.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR



# setup code
cd ../../src
make clean
make all

# record results of tests
# echo ""
# echo "running test 16"
# ./server > $STUDENT_OUTPUT_DIR/test16gen.server.debug.out &
# sleep 1
# valgrind --tool=cachegrind ./client < ../$REL_TEST_DIR/test16gen.dsl
# echo "running test 17"
# ./server > $STUDENT_OUTPUT_DIR/test17gen.server.debug.out &
# sleep 1
# valgrind --tool=cachegrind ./client < ../$REL_TEST_DIR/test17gen.dsl

echo ""
echo "running test 18"
./server > $STUDENT_OUTPUT_DIR/test18gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test18gen.dsl
echo "running test 19"
 ./server > $STUDENT_OUTPUT_DIR/test19gen.server.debug.out &
sleep 1
 ./client < ../$REL_TEST_DIR/test19gen.dsl
# echo ""
# echo "running test 26"
# valgrind --tool=cachegrind --branch-sim=yes --log-file=test.out ./server &
# sleep 1
# ./client < ../$REL_TEST_DIR/test26gen.dsl
# echo "running test 27"
# valgrind --tool=cachegrind --branch-sim=yes --log-file=test2.out ./server &
# sleep 1
#  ./client < ../$REL_TEST_DIR/test27gen.dsl


echo ""
echo "running test 18"
./server > $STUDENT_OUTPUT_DIR/test101gen.server.debug.out &
sleep 1
time ./client < ../$REL_TEST_DIR/test26gen.dsl
echo "running test 19"
 ./server > $STUDENT_OUTPUT_DIR/test102gen.server.debug.out &
sleep 1
 time ./client < ../$REL_TEST_DIR/test27gen.dsl

if pgrep server; then pkill server; fi
