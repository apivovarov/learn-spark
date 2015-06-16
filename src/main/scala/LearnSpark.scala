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

    // 1. Parse transactions, filter by date and group by cid + yyyy-MM
    val trans = sc.textFile(inDir + "trans.txt").map(line => line.split(";"))
    val transY = trans.filter(_(1).startsWith("2015"))
    val transGrp = transY.map(trans => ((trans(0), trans(1).substring(0, 7)), trans(2).toDouble)).reduceByKey(_ + _)

    // 2. Parse customers
    val custs = sc.textFile(inDir + "cust.txt").map(_.split(";"))

    // 3. Join aggregated transactions and customers
    val transCust = transGrp.map(trans => (trans._1._1, trans)).leftOuterJoin(custs.map(cust => (cust(0), cust(1))))
    val res = transCust.map(txCust => ((txCust._2._1._1._2, txCust._1), Array(txCust._2._1._1._2, txCust._1, txCust._2._2.getOrElse(""), "%.2f".format(txCust._2._1._2)).mkString(";")))
    res.sortByKey(true, 1).values.saveAsTextFile(outDir)

    // clean up
    sc.stop()
  }
}
