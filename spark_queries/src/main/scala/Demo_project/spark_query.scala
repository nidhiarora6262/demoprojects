package Demo_project
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

/*trait queries{
  def schema(){


}*/

object spark_query {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("example of SparkSession")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val data = spark.read.option("header", "true").csv("/home/nidhi/Documents/datanew.csv")
    data.printSchema()
    data.show()


    //readFile(spark: SparkSession)
    /* val data2 = data.withColumn("Id", col("Id").cast(DoubleType))
      .withColumn("Marks", col("Marks").cast(DoubleType))
      .withColumn("phoneno", col("phoneno").cast(DoubleType))
    //.withColumn("",data("Marks").cast(DoubleType))
    data2.printSchema()

  }*/
  }
}










 /* def readFile(spark: SparkSession) {
    val data = spark.read.option("header", "true").csv("/home/nidhi/Documents/datanew.csv")
    data.show()


  }*/






  /*def count(Name:String,Stream:String):Any=
  {
    val data7= data.groupBy("Name", "Stream").count().show()




  }

}
*/