${SPARK_HOME}/bin/spark-submit \
  --class "LearnSpark" \
  target/scala-2.10/learn-spark_2.10-1.0.jar \
  "$@"
