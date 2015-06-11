${SPARK_HOME}/bin/spark-submit \
  --class "LearnSpark" \
  target/scala-2.11/learn-spark_2.11-1.0.jar \
  "$@"
