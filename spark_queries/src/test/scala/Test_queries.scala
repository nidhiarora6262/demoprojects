import Demo_project.Spark_Class
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

//import Demo_project.spark_queries
//import org.testng.Assert._


class Test_queries extends FunSuite {
  var spark: SparkSession = _

  //val data1 = spark.read.option("header", "true").csv("/home/nidhi/Documents/datanew.csv")
  def beforeEach() {
    spark = new SparkSession.Builder().appName("Spark Job for Loading Data").master("local[*]").getOrCreate()
  }

  test("testing RDD") {
    val data1 = spark.read.option("header", "true").csv("/home/nidhi/Desktop/nidhiarora/spark_queries/src/main/resources/FL_insurance_sample.csv")
    assert(data1.count() != 0)
  }




//  test("casting") {
//
//    val abc = spark_queries.schema1(data1,"50","123","12","34","34","67","78","12","11","56")
//    assert ( data.schema === abc.schema)
//  }

  test("groupBY") {

    val ans = Spark_Class.groupby(,"1","3","4")
    assert(ans.count > 1)

  }


  /*test("unpivot") {
    val ans1 = spark_queries.unpivot(data1,"2", "3","34","45","67","67",4,5,8,7,9,4,7,8,7,9)
    assert(ans1.collect() === 5)

  }*/
}


