# learn-spark
## Spark examples


### The Data

The data is located in in and contain the following files:


## Build and Run

### Build
Use sbt to build the project
```
$ sbt package
```
### Run
Use run.sh with 2 parameters `<in_dir>` and `<out_dir>` to submit app to Spark. `<out_dir>` should not exist before run
```
$ ./run.sh <in_dir> <out_dir>
```
Output should be in `<out_dir>/part-00000` file
