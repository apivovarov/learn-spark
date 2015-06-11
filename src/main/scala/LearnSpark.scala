/* LearnSpark.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

object LearnSpark {
  /**
   * Function to explode actors (return new actor record for each actor's email)
   */
  def explodeActor(actor: Array[String]) : Seq[Array[String]] = {
    val phArr = actor(3).split(",")
    val acts = new ListBuffer[Array[String]]()
    for (ph <- phArr) {
      acts += Array(actor(0), actor(1), ph)
    }
    return acts.toList
  }

  def addSeparator(dir : String) : String = {
    val sep = System.getProperty("file.separator")
    var res = dir
    if (!res.endsWith(sep)) {
      res += sep
    }
    return res
  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: LearnSpark <in_dir> <out_dir>")
      System.err.println("<out_dir> should not exist before run")
      System.exit(1)
    }

    // input and output files folder
    val inDir = addSeparator(args(0))
    val outDir = addSeparator(args(1))

    val conf = new SparkConf().setAppName("Learn Spark")
    val sc = new SparkContext(conf)

    // clean up
    sc.stop()
  }
}
