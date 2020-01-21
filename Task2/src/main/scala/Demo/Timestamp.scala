
package Demo
import com.google.common.base.Functions
import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lead, _}
class Windows {


  val spark = SparkSession.builder().master("local[*]").appName("example of SparkSession")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

   //read the data
  val words = spark.read.option("header", "true").csv("/home/nidhi/Desktop/sample_input.csv")
  words.show()

  def range(DEVICE_ID: Int, TIMESTAMP: String, FLOWRATE: Double): DataFrame = {

    val window = Window.partitionBy("DEVICE_ID").orderBy("TIMESTAMP")

    val df = words.withColumn("previous", functions.lag("FLOW_RATE", 1).over(window)).
      withColumn("next", functions.lead("FLOW_RATE", 0).over(window))
    df.show(30)

    //df.filter("previous == 0 and next == 0 and FLOW_RATE ==0" ).drop().show()

    ////val df8=df.select("DEVICE_ID","FLOW_RATE","TIMESTAMP","PRESSURE","MODE","VALVE_STATE","previous","next").where(" FLOW_RATE !=0.0D and  previous != 0.0D and next != 0.0D")
    //val df8=df.filter(" COALESCE(previous and  next and FLOW_RATE) IS NOT NULL")
    //df8.show()
    //df.filter("df.previous isNull and df.next isNull  and df.FLOW_RATE isNull").drop().show()
    //df.drop((col("previous") == 0 && col("next")  == 0&& col("FLOW_RATE") ==0)).show()

    // df.select("next").show()

    val df2 = df.withColumn("start_flowrate", functions.when(functions.col("previous") > 0.0D and functions.col("next") >= 0.0D, 0).otherwise(1.0))
      .withColumn("end_flowrate", functions.when(functions.col("next") equalTo 0.0D and functions.col("previous") > 0.0D, 0).otherwise(1.0))
    df2.show(30)


    val df3 = df2.withColumn("previousstartflowrate", functions.lag("start_flowrate", 1).over(window))
    df3.show(30)

    val df4 = df3.withColumn("starttimestamp", functions.when(functions.col("previous") > 0.0D and functions.col("previousstartflowrate") > 0.0D,df2.col("TIMESTAMP")))
    df4.show(30)


    val df6 = df4.withColumn("previousendflowrate", functions.lag("end_flowrate", 1).over(window))
    df6.show(30)
    val df7 = df6.withColumn("endtimestamp", functions.when(functions.col("end_flowrate") equalTo 0.0D and functions.col("previousendflowrate") > 0.0D,df2.col("TIMESTAMP")))
    df7.show(200)
    //df10.withColumn("finaltimestamp2",functions.col(df10.select("startTimestamp").distinct(). as ("starttimestamp1"))).show()
    //df10.withColumn("finaltimestamp1",functions.col(df10.select("starttimestamp").distinct()

    //df10.withColumn("finaltimestamp2",functions.col(df10.select("startTimestamp").distinct(). as ("starttimestamp1"))).show()
    val window2 = Window.partitionBy("DEVICE_ID").orderBy("TIMESTAMP")
    //df10.withColumn("finaltimestamp2",functions.col(df10.select("startTimestamp").distinct(). as ("starttimestamp1"))).show()
    val df8=df7.withColumn("finaltimestamp1",first("starttimestamp", true).over(window)).
      withColumn("finaltimestamp2",last("endtimestamp", true).over(window2))

    df8.show()

    df8.groupBy( "finaltimestamp1","finaltimestamp2" ).count().show()




      //fill that with timestamp

    //final timestamp
   // df5.select("starttimestamp1").distinct.as("Finalstarttimestamp").show()
    //df8.select("endtimestamp1").distinct.as("Finalendtimestamp").show()

    //df=window.partitionBy("DEVICE_ID").orderBy("DEVICE_ID")

    //for month column
    df.select(date_format(col("TIMESTAMP"), "yyyy-MM-dd").alias("Month").cast("date"))
    df
  }
}
    object Timestamp  extends Windows {
   def main(args:Array[String]) :Unit={


     val a = new Windows()
     a.range(1, "2019,23,2", 0.5)


   }

  }


//val df8 = df7.withColumn("endtimestamp1", when(df7.col("endtimestamp") isNull, 0).otherwise(df2.col("TIMESTAMP")))
//df8.show()

//df2.select("start_flow").show()
/* val df3 = df2.withColumn("timestamp1",

   when(df2.col("start_flowrate") isNull, 0).otherwise(df2.col("TIMESTAMP")))
   .withColumn("timestamp2",
     when(df2.col("end_flowrate") isNull, 0).otherwise(df2.col("TIMESTAMP")))
 df3.show()
 val window2 = Window.orderBy("DEVICE_ID")
 val df4 = df3.withColumn("timestamp5", functions.lead("timestamp1", 1).over(window2))
 df4.show()

 df4.select("timestamp5").show()
 df4
*/
//fill that with timestamp
//val df5 = df4.withColumn("starttimestamp1", when(df4.col("starttimestamp") isNull, value = 0).otherwise(df2.col("TIMESTAMP")))
//df5.show()

// val df5 = functions.when(df4.col("timestamp1") isNull, df4.select(first("timestamp5")).as("newtimestamp"))





// val overCategory = Window.partitionBy("TIMESTAMP")
//df.withColumn("start", F.coalesce(F.lag(col("start_flowrate"), 1).over(orderBy(col("devideId"))

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








