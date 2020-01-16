
package Demo
import com.google.common.base.Functions
import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lead, _}
class Window {


  val spark = SparkSession.builder().master("local[*]").appName("example of SparkSession")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()


  val words = spark.read.option("header", "true").csv("/home/nidhi/Documents/input.csv")
  words.show()

  def range(DEVICE_ID: Int, TIMESTAMP: String, FLOWRATE: Double): DataFrame = {
    val window = Window.orderBy("DEVICE_ID")
    /*val lagCol = lag(col("flowrate"), 1).over(window)
    //words.withColumn("LagCol", lagCol).show()

    //val leadCol = lead(col("flowrate"), 1).over(window)
    //words.withColumn("LeadCol", leadCol).show()*/

    val df = words.withColumn("previous", functions.lag("FLOW_RATE", 1).over(window)).
      withColumn("next", functions.lead("FLOW_RATE", 1).over(window))
    df.show()


    val df2 = df.withColumn("start_flowrate", functions.when(functions.col("previous") > 0.0D and functions.col("next") >= 0.0D, 0.0D)).
      withColumn("end_flowrate", functions.when(functions.col("next") equalTo 0.0D, 0.0D))
    df2.show()

    //df2.select("start_flow").show()
    val df3 = df2.withColumn("timestamp1",

      when(df2.col("start_flowrate") isNull, 0).otherwise(df2.col("TIMESTAMP")))
      .withColumn("timestamp2",
        when(df2.col("end_flowrate") isNull, 0).otherwise(df2.col("TIMESTAMP")))
    df3.show()
    val window2 = Window.orderBy("DEVICE_ID")
    val df4 = df3.withColumn("timestamp5", functions.lead("timestamp1", 1).over(window2))
    df4.show()

    df4.select("timestamp5").show()
    df4
   // val df5 = functions.when(df4.col("timestamp1") isNull, df4.select(first("timestamp5")).as("newtimestamp"))
  }
}




    // val overCategory = Window.partitionBy("TIMESTAMP")
    //df.withColumn("start", F.coalesce(F.lag(col("start_flowrate"), 1).over(orderBy(col("devideId"))



  object Timestamp  extends Window {
   def main(args:Array[String]) :Unit={


     val a = new Window()
     a.range(1, "2019,23,2", 0.5)


   }

  }
// val w = Window.partitionBy("TIMESTAMP").orderBy("DEVICE_ID")
//functions.collect_list().over(w)


// val states = timestamp1.rdd.map(x=>x(0)).collect.toList()



//.filter(df.timestamp1.isNull()).drop()
//df3.filter(df3("timestamp1").isNull).drop() .show()


//.filter(df.col_X.isNull()).drop()


//val df2=Window.partitionBy("Timestamp")

      // val df2=words.select("Timestamp")

      //val window2=Window.orderBy("end")
      //df.withColumn("strttime", first("Timestamp").over(window2))
      // .withColumn("endtime", last("Timestamp").over(window2)).show()
      //df.where("start == 0.0D").select("Timestamp").distinct.show()

     //




      //df2.withColumn("starttimestamp",words.select("Timestamp").where("start )


      //(functions.col("next") > 0).otherwise(functions.lit(0))))






      //df.withColumn("start",).filter("previous is ").show()


      // words.createOrReplaceGlobalTempView("people")


      //words.foreach{a => a foreach println}
      //df.select("Id").where("previous.isNotNull || next.isNull").show()

      //val df2=words.select("")







