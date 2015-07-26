/* PageRank.scala */

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank {

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: PageRank in/pages.txt")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))
    val arr = lines.map(_.split('|'))
    val linksRaw = arr.map(x => (x(0), x(1))).mapValues(_.split(','))
    val links = linksRaw.partitionBy(new HashPartitioner(3)).persist()

    var ranks = links.mapValues(x => 1.0)

    for (i <- 0 until 10) {
      val joined = links.join(ranks)
      val contrib = joined.flatMap {
        case (page, (links, rank)) =>
          links.map(link => (link, rank / links.size))
      }
      ranks = contrib.reduceByKey((a, b) => a + b).mapValues(v => 0.15 + 0.85 * v)
    }

    ranks.foreach(println)

    // clean up
    sc.stop()
  }
}
