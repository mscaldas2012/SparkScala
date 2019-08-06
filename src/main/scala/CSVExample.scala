import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object CSVExample {

  def main(args:Array[String]) =  {
    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate

   processCSV(spark)
   // processKafka(spark)

  }

  def calcSS(df: DataFrame ): String = {

    //find the closest record by dtspec and sertype is not null
    //println("----\n\n" +
   // df.flatMap()
    //state.getItem(0)
    //return s"${state}"
    return null
  }

  def processCSV(spark: SparkSession) = {
    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("src/main/resources/fddcases.csv")

    //df.show()

//    val nonNullSS = df.withColumn("calcSS",
//                      when(col("serotypeSummary").isNotNull, col("serotypeSummary")).
//                      otherwise(calcSS(df))
//                    )
//    nonNullSS.show()

    df.createOrReplaceTempView("FDD_INCIDENTS")
       val fullQ = spark
//                      .sql("SELECT o.state,o.patId,o.serotypeSummary " +
//                                   " From FDD_INCIDENTS o")

           .sql("SELECT  serotypeSummary  " +
                        " FROM FDD_INCIDENTS o   " +
                        " WHERE o.state='GA' and o.patID='P123' and serotypeSummary is not null" +
                        " ORDER BY ABS(dtSpec)")
//         .sql("SELECT o.state,o.patId,o.serotypeSummary, " +
//           "      CASE WHEN serotypeSummary != null THEN o.serotypeSummary " +
//           "        ELSE(SELECT TOP 1 int.serotypeSummary " +
//           "               FROM FDD_INCIDENTS int " +
//           "              WHERE o.state=int.state and o.patID=int.patID and int.serotypeSummary is not null " +
//           "              ORDER BY ABS(DATEDIFF(d,o.dtSpec,int.dtSpec)))end as TEMPSS " +
//           " From FDD_INCIDENTS o")


    fullQ.show()

  }

  def processKafka(spark: SparkSession): Unit = {
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "fddcases")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
    val newdf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    newdf.show()



    val dfContent = newdf.select(explode(newdf("value"))).toDF("valueid")
    dfContent.show()
//
//    val schema = StructType(Seq(
//      StructField("k", StringType, true), StructField("v", StringType, true)
//    ))
//    newdf.withColumn("valueJson", from_json("value", schema, Map[String, String]()))
//
////    val stringDf = df.map((value: Row) => value.getString(0), Encoders.STRING)
////    spark.read.json(stringDf)

  }
}


