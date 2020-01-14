package Demo
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lead, _}

  object Timestamp {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().master("local[*]").appName("example of SparkSession")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

      val words = spark.read.option("header", "true").csv("/home/nidhi/Documents/animals.csv")
      words.show()

      val window = Window.partitionBy("Timestamp").orderBy("Id")
      /*val lagCol = lag(col("flowrate"), 1).over(window)
      //words.withColumn("LagCol", lagCol).show()

      //val leadCol = lead(col("flowrate"), 1).over(window)
      //words.withColumn("LeadCol", leadCol).show()*/

      val df= words.withColumn("previous",functions.lag("flowrate",1).over(window)).
        withColumn("next",functions. lead("flowrate",1).over(window))
      df.show()



      //words.foreach{a => a foreach println}
      //df.select("Id").where("previous.isNotNull || next.isNull").show()

      //val df2=words.select("")






  }

}
