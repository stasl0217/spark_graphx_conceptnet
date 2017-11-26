import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object Task3 {
  def main(args: Array[String]) {
    if (System.getProperty("os.name").startsWith("Windows"))
    {
      System.setProperty("hadoop.home.dir", "C:\\Users\\Administrator\\.m2\\repository\\org\\apache\\hadoop\\winutils")
    }


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("GraphX")
    //    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val tweetData = sc.textFile("s3://cmucc-datasets/p42/Graph")


    val sqlContext = new SQLContext(sc)
    val df_vertices = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("vertices.csv")
      .select(col("vid"), col("vertex"))
    //    "file:///home/clash/shirley/story_comprehension/vertices.csv"

    // extract vertices data from pagerank result
    // each vertice: (uid, (most influential followee's uid, most influential followee's rank))
    df_vertices.show()
    val vertices_rdd = df_vertices.rdd
    vertices_rdd.take(10).foreach(println)
    val vertices : RDD[(VertexId, (String, String))] = vertices_rdd.map
    {
      components =>
        (components(0), (components(1))) // initialize followee's uid and rank as 0L, 0.0
    }.persist()
    vertices.take(10).foreach(println)


    val df_edge = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("edges.csv").select(col("v1_id"), col("v2_id"), col("weight"))
      .persist()
    df_edge.show()
  }
}
