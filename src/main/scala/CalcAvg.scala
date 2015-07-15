/* CalcAvg.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

object CalcAvg {

  def addSeparator(dir : String) : String = {
    val sep = System.getProperty("file.separator")
    var res = dir
    if (!res.endsWith(sep)) {
      res += sep
    }
    return res
  }

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: LearnSpark <in_dir>")
      System.exit(1)
    }

    // input and output files folder
    val inDir = addSeparator(args(0))

    val conf = new SparkConf().setAppName("CalcAvg")
    val sc = new SparkContext(conf)

    // 1. readFile
    // val inDir = "in/"
    val nums = sc.textFile(inDir + "numbers.txt").map(line => line.toDouble)

    // 2. aggregate
    val agg = nums.aggregate((0.0, 0))(
      (acc, v) => (acc._1 + v, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // 3. calc avg and print
    val avg = agg._1 / agg._2
    println(f"$avg%1.8f")

    // clean up
    sc.stop()
  }
}
