package Demo_project
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import Demo_project.spark_queries.spark
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
//import Demo_project.spark_query.data
import org.apache.spark.sql.types._

class spark
{

  /*def schema1(Id: String, Marks: String, phoneno: String): DataFrame= {

      val data2 = data.withColumn("Id", data("Id").cast(DoubleType))
      .withColumn("Marks", data("Marks").cast(DoubleType))
      .withColumn("phoneno", data("phoneno").cast(DoubleType))
    //.withColumn("",data("Marks").cast(DoubleType))
       return data2
       }*/



  def unpivot(data: DataFrame, policyId:String,statecode:String, county:String, construction:String,point_latitude:String, point_longitude:String,
    eq_site_limit:Double, hu_site_limit:Double, fl_site_limit: Double, fr_site_limit:Double, tiv_2011:Double, tiv_2012:Double, eq_site_deductible:Double
    ,hu_site_deductible:Double, fl_site_deductible:Double, fr_site_deductible:Double ) {
    val DF4 = data.selectExpr("policyId", "statecode", "county", "construction", "point_latitude",
      "point_longitude", "stack(10, 'eq_site_limit',eq_site_limit, 'hu_site_limit',hu_site_limit, 'fl_site_limit',fl_site_limit," +
        " 'fr_site_limit',fr_site_limit,'tiv_2011',tiv_2011,' tiv_2012',tiv_2012, 'eq_site_deductible',eq_site_deductible,' hu_site_deductible', hu_site_deductible," +
        "'fl_site_deductible',fl_site_deductible, 'fr_site_deductible', fr_site_deductible) as (LimitValue,Limitcode)")
      .where("Limitcode is not null")
       DF4.show()
      //spark.tim(DF4.show())




    val df3 = DF4.groupBy("policyId", "statecode", "county", "Limitcode").agg(count("Limitvalue")).withColumn("processing_datetime_utc", lit((0))).show()

    val df7 = DF4.groupBy("point_longitude", "point_latitude").count().show()

  }


    def groupby (data:DataFrame,  policyId:String,  point_latitude:String, point_longitude:String     ) {

      val df8 = data.groupBy("policyID").agg(collect_list("point_longitude"), collect_list("point_latitude")).show()


    }


  }






  object spark_queries  extends spark
{
def main (args: Array[String]): Unit = {




  val spark = SparkSession.builder ().master ("local[*]").appName ("example of SparkSession")
  .config ("spark.some.config.option", "some-value")
  .getOrCreate ()


  val data = spark.read.option ("header", "true").csv ("/home/nidhi/Desktop/FL_insurance_sample.csv")
  data.printSchema ()
  data.show ()

  val a = new spark()
   a.groupby( data ,"1","2","3")

     a.unpivot(data,"1","2","3","4","5","6",2.0,3.0,2,3,4,4,6,5,5,3)




}
  }





