 
### This is a test dummy example of running a couple generated tests ###

#### TO BE RUN INSIDE DOCKER #####
# run from base dirctory as ./infra_scripts/test.sh

# the test directory, containing generated data dl, exp, and csvs
# expressed, relative to the base project directory
REL_TEST_DIR=generated_data
# absolute with the docker mount points
ABS_TEST_DIR=/cs165/$REL_TEST_DIR

STUDENT_OUTPUT_DIR=/cs165/student_outputs

DATA_SIZE=1000000
RAND_SEED=42

# create milestone 1 data
cd project_tests/data_generation_scripts
python exp1.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR $ABS_TEST_DIR



# setup code
cd ../../src
make clean
make all

# record results of tests
echo ""
echo "running test 51"
./server > $STUDENT_OUTPUT_DIR/test51gen.server.debug.out &
sleep 1
time ./client < ../$REL_TEST_DIR/test51gen.dsl
echo "running test 51"
./server > $STUDENT_OUTPUT_DIR/test51gen.server.debug.out &
sleep 1
time ./client < ../$REL_TEST_DIR/test51gen.dsl



if pgrep server; then pkill server; fi
