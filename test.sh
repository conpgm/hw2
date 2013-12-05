#!/bin/bash

Time1=`date +%s%N | cut -b1-13`
java -jar tupletest.jar -i <<EOF
n
n
n
n
y
q
EOF
Time2=`date +%s%N | cut -b1-13`
echo -e "\n**************"
echo "Test:" `expr $Time2 - $Time1` "ms"
echo -e "**************\n"


Time1=`date +%s%N | cut -b1-13`
java -jar tupletest.jar -i <<EOF
n
n
n
n
n
n
n
n
n
n
y
q
EOF
Time2=`date +%s%N | cut -b1-13`
echo -e "\n**************"
echo "Test:" `expr $Time2 - $Time1` "ms"
echo -e "**************\n"
