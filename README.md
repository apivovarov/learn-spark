### Spark examples

#### Build
```
$ sbt package
```

#### Sample Data

Sample data is located in `in` folder

#### Run Learn Spark
The program to join cust.txt with trans.txt data. Sample data is located in `in` folder.

`<out_dir>` should not exist before
 run
```
$ ./learn-spark.sh in out
```
Output should be in `<out_dir>/part-00000` file

#### Run CalcAvg
Calculates average for numbers in file
```
$ ./calc-avg.sh in/numbers.txt
```
Avg value will be printed to std out

#### Run PageRank
Calculates page rank for pages in in/pages.txt.

File rormat `<src_page>|<dest_page_1>,<dest_page_2>,<dest_page_n>`
```
$ ./page-rank.sh in/pages.txt
```
Page ranks will be printed to std out
