
#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd

import data_gen_utils
import math

# note this is the base path where we store the data files we generate
TEST_BASE_DIR = "/cs165/generated_data"

# note this is the base path that _POINTS_ to the data files we generate
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"
#
# Example usage: 
#   python milestone1.py 10000 42 ~/repo/cs165-docker-test-runner/test_data /cs165/staff_test
#

# PRECISION FOR AVG OPERATION
PLACES_TO_ROUND = 2

############################################################################
# Notes: You can generate your own scripts for generating data fairly easily by modifying this script.
############################################################################

def generateDataFileMidwayCheckin():
    outputFile = TEST_BASE_DIR + '/data1_generated.csv'
    header_line = data_gen_utils.generateHeaderLine('db1', 'tbl1', 2)
    column1 = list(range(0,1000))
    column2 = list(range(10,1010))
    #### For these 3 tests, the seed is exactly the same on the server.
    np.random.seed(47)
    np.random.shuffle(column2)
    #outputTable = np.column_stack((column1, column2)).astype(int)
    outputTable = pd.DataFrame(list(zip(column1, column2)), columns =['col1', 'col2'])
    outputTable.to_csv(outputFile, sep=',', index=False, header=header_line, line_terminator='\n')
    return outputTable

def generateDataFile2(dataSizeTableTwo):
    outputFile = TEST_BASE_DIR + '/' + 'exp1_generated.csv'
    header_line = data_gen_utils.generateHeaderLine('db1', 'tbl2', 4)
    outputTable = pd.DataFrame(np.random.randint(-1 * dataSizeTableTwo/2, dataSizeTableTwo/2, size=(dataSizeTableTwo, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    outputTable['col2'] = outputTable['col2'] + outputTable['col1']
    # This is going to have many, many duplicates!!!!
    outputTable['col3'] = np.random.randint(0,100, size = (dataSizeTableTwo))
    outputTable['col4'] = np.random.randint(2**31 - 10000, 2**31, size = (dataSizeTableTwo))
    outputTable.to_csv(outputFile, sep=',', index=False, header=header_line, line_terminator='\n')
    return outputTable


def createTestFour(dataTable):
    # prelude
    output_file, exp_output_file = data_gen_utils.openFileHandles(51, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Load Test Data 2\n')
    output_file.write('--\n')
    output_file.write('-- Load+create+insert Data and shut down of tbl2 which has 4 attributes\n')
    output_file.write('create(tbl,\"tbl2\",db1,4)\n')
    output_file.write('create(col,\"col1\",db1.tbl2)\n')
    output_file.write('create(col,\"col2\",db1.tbl2)\n')
    output_file.write('create(col,\"col3\",db1.tbl2)\n')
    output_file.write('create(col,\"col4\",db1.tbl2)\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/data2_generated.csv\")\n')
    output_file.write('relational_insert(db1.tbl2,-1,-11,-111,-1111)\n')
    output_file.write('relational_insert(db1.tbl2,-2,-22,-222,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-3,-33,-333,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-4,-44,-444,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-5,-55,-555,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-6,-66,-666,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-7,-77,-777,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-8,-88,-888,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-9,-99,-999,-2222)\n')
    output_file.write('relational_insert(db1.tbl2,-10,-11,0,-34)\n')
    output_file.write('shutdown\n')

    # columns need to align for append to work
    deltaTable = pd.DataFrame([[-1, -11, -111, -1111],
        [-2, -22, -222, -2222],
        [-3, -33, -333, -2222],
        [-4, -44, -444, -2222],
        [-5, -55, -555, -2222],
        [-6, -66, -666, -2222],
        [-7, -77, -777, -2222],
        [-8, -88, -888, -2222],
        [-9, -99, -999, -2222],
        [-10, -11, 0, -34]], columns=['col1', 'col2', 'col3', 'col4'])
    
    dataTable = dataTable.append(deltaTable)
    data_gen_utils.closeFileHandles(output_file, exp_output_file)
    return dataTable

def generateDataMilestone3(dataSize):
    outputFile_ctrl = TEST_BASE_DIR + '/' + 'data4_ctrl.csv'
    outputFile_btree = TEST_BASE_DIR + '/' + 'data4_btree.csv'
    outputFile_clustered_btree = TEST_BASE_DIR + '/' + 'data4_clustered_btree.csv'
    header_line_ctrl = data_gen_utils.generateHeaderLine('db1', 'tbl4_ctrl', 4)
    header_line_btree = data_gen_utils.generateHeaderLine('db1', 'tbl4', 4)
    header_line_clustered_btree = data_gen_utils.generateHeaderLine('db1', 'tbl4_clustered_btree', 4)
    outputTable = pd.DataFrame(np.random.randint(0, dataSize/5, size=(dataSize, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    # This is going to have many, many duplicates for large tables!!!!
    outputTable['col1'] = np.random.randint(0,1000, size = (dataSize))
    outputTable['col4'] = np.random.randint(0,10000, size = (dataSize))
    ### make ~5\% of values a single value! 
    maskStart = np.random.uniform(0.0,1.0, dataSize)   
    mask1 = maskStart < 0.05
    ### make ~2% of values a different value
    maskStart = np.random.uniform(0.0,1.0, dataSize)
    mask2 = maskStart < 0.02
    outputTable['col2'] = np.random.randint(0,10000, size = (dataSize))
    frequentVal1 = np.random.randint(0,int(dataSize/5))
    frequentVal2 = np.random.randint(0,int(dataSize/5))
    outputTable.loc[mask1, 'col2'] = frequentVal1
    outputTable.loc[mask2, 'col2'] = frequentVal2
    outputTable['col4'] = outputTable['col4'] + outputTable['col1']
    outputTable.to_csv(outputFile_ctrl, sep=',', index=False, header=header_line_ctrl, line_terminator='\n')
    outputTable.to_csv(outputFile_btree, sep=',', index=False, header=header_line_btree, line_terminator='\n')
    outputTable.to_csv(outputFile_clustered_btree, sep=',', index=False, header=header_line_clustered_btree, line_terminator='\n')
    return frequentVal1, frequentVal2, outputTable
## NOTE: approxSelectivity should be between 0 and 1
def createTestFive(dataTable, dataSizeTableTwo, approxSelectivity):
    # prelude
    output_file26, exp_output_file26 = data_gen_utils.openFileHandles(101, TEST_DIR=TEST_BASE_DIR)
    output_file27, exp_output_file27 = data_gen_utils.openFileHandles(102, TEST_DIR=TEST_BASE_DIR)
    #output_file26, exp_output_file = data_gen_utils.openFileHandles(filenum, TEST_DIR=TEST_BASE_DIR)
    # output_file.write('-- Summation\n')
    # output_file.write('--\n')
    # query
    offset = int(approxSelectivity * dataSizeTableTwo)
    highestHighVal = int((dataSizeTableTwo/2) - offset)
    # selectValLess = np.random.randint(int(-1 * (dataSizeTableTwo/2)), highestHighVal)
    # selectValGreater = selectValLess + offset
   # output_file.write('-- SELECT SUM(col3) FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
    print("YUH")
    for i in range(100):
        selectValLess = np.random.randint(0, highestHighVal)
        selectValGreater = selectValLess + offset
        #val1 = np.random.randint(0, int((dataSize/5) - offset))
        output_file26.write('s{}=select(db1.tbl4_ctrl.col2,{},{})\n'.format(i, np.array(dataTable['col2'])[selectValLess], np.array(dataTable['col2'])[selectValGreater]))
        output_file26.write('f{}=fetch(db1.tbl4_ctrl.col3,s{})\n'.format(i,i))
        output_file26.write('a{}=avg(f{})\n'.format(i,i))
        output_file26.write('print(a{})\n'.format(i))
        output_file27.write('s{}=select(db1.tbl4.col2,{},{})\n'.format(i, np.array(dataTable['col2'])[selectValLess], np.array(dataTable['col2'])[selectValGreater]))
        output_file27.write('f{}=fetch(db1.tbl4.col3,s{})\n'.format(i,i))
        output_file27.write('a{}=avg(f{})\n'.format(i,i))
        output_file27.write('print(a{})\n'.format(i))
    # for i in range(100):
    #     output_file.write('s1=select(db1.tbl2.col1,{},{})\n'.format(selectValLess, selectValGreater))
    #     output_file.write('f1=fetch(db1.tbl2.col3,s1)\n')
    #     output_file.write('f1=fetch(db1.tbl2.col2,s1)\n')
        # generate expected results
    print("mann")
    data_gen_utils.closeFileHandles(output_file26, exp_output_file26)
    data_gen_utils.closeFileHandles(output_file27, exp_output_file27)





def generateTestsMidwayCheckin(dataTable):
    createTestOne()
    createTestTwo(dataTable)
    createTestThree(dataTable)

def generateOtherMilestoneOneTests(dataTable2, dataSizeTableTwo):
    dataTable2 = createTestFour(dataTable2)
    createTestFive(dataTable2, dataSizeTableTwo, 0.5)
    # createTestFive(dataTable2, dataSizeTableTwo, 0.05,52)

    # createTestFive(dataTable2, dataSizeTableTwo, 0.1,53)
    # createTestFive(dataTable2, dataSizeTableTwo, 0.2,54)

    # createTestFive(dataTable2, dataSizeTableTwo, 0.3,55)
    # createTestFive(dataTable2, dataSizeTableTwo, 0.4,56)

    # createTestFive(dataTable2, dataSizeTableTwo, 0.5,57)
    # createTestFive(dataTable2, dataSizeTableTwo, 0.6,58)

    # createTestFive(dataTable2, dataSizeTableTwo, 0.7,59)
    # createTestFive(dataTable2, dataSizeTableTwo, 0.8,60)



def generateMilestoneOneFiles(dataSizeTableTwo, randomSeed):
    #### The seed is now a different number on the server! Data size is also different.
    np.random.seed(randomSeed)
    _,_,dataTable2 = generateDataMilestone3(dataSizeTableTwo)
    generateOtherMilestoneOneTests(dataTable2, dataSizeTableTwo)

def main(argv):
    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR

    dataSizeTableTwo = int(argv[0])

    print(argv)
    if len(argv) > 1:
        randomSeed = int(argv[1])
    else:
        randomSeed = 47

    # override the base directory for where to output test related files
    if len(argv) > 2:
        TEST_BASE_DIR = argv[2]
        if len(argv) > 3:
            DOCKER_TEST_BASE_DIR = argv[3]


    generateMilestoneOneFiles(dataSizeTableTwo, randomSeed)

if __name__ == "__main__":
    main(sys.argv[1:])
