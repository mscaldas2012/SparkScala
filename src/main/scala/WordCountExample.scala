import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCountExample {

  def main(args:Array[String]) =  {
//    val spark = SparkSession.builder
//      .master("local")
//      .appName("Spark Word Counter")
//      .getOrCreate

    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)

    //val input = spark.read.option("delimiter", "\n").csv("docker-compose.yaml").toJavaRDD

    val input = sc.textFile("docker-compose.yaml")
    val words = input.flatMap(line => line.split("[ =:]"))
    val counts = words.map(word => (word, 1)).reduceByKey{ case (x,y) => x+y}

    counts.saveAsTextFile("output.txt")

  }

}
