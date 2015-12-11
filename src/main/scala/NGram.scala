import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NGram {

  def nGram(words: Array[String]): Array[String] = {
    val result = new ArrayBuffer[String]()
    if (words.length <= 1) {
      result.toArray
    } else {
      for (i <- 0 until (words.length-1)) {
        result += (words(i) + words(i+1))
      }
      result.toArray
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("n-gram")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("hdfs://127.0.0.1:9000/sogou2", 4)
    val topWords = textFile.flatMap{
      line => nGram(line.split(""))
    }.map{
      word => (word, 1)
    }.reduceByKey{
      (a, b) => a + b
    }.sortBy(
      x => -x._2
    ).cache()

    topWords.take(1000).foreach {
      word => println(word)
    }

    println("total 2-gram words : " + topWords.count())
  }

}
