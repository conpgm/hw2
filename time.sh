Time1=`date +%s%N | cut -b1-13`
java -jar tupletest.jar
Time2=`date +%s%N | cut -b1-13`
echo "\nTime:" `expr $Time2 - $Time1` "ms"
