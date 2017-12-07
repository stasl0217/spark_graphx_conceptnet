import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths

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
    import sqlContext.implicits._
    val df_vertices = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("vertices_small.csv")
      .select(col("vid"), col("vertex"))
    //    "file:///home/clash/shirley/story_comprehension/vertices_small.csv"

    // extract vertices data from pagerank result
    // each vertice: (uid, (most influential followee's uid, most influential followee's rank))
    df_vertices.show()
    val vertices = df_vertices.as[(VertexId,String)].rdd

    vertices.take(10).foreach(println)



    val df_edges = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("edges.csv").select(col("srcId"), col("dstId"), col("attr"))
      .persist()
    df_edges.printSchema()  // [srcId, dstId, attr]
    val edges1 = df_edges.as[Edge[Int]].rdd

    // make it undirected
    val inverted_edges =df_edges.toDF(Seq("dstId","srcId","attr"):_*)
    val edges2 = inverted_edges.as[Edge[Int]].rdd
//    edges2.foreach(println)

    val edges = edges1.union(edges2)

    val graph = Graph(vertices, edges)

//    val dests = sc.textFile("dests_small.csv").map(x => VertexId(x.toInt)).collect().toSeq
//    dests.foreach(println)

    val df_dests = sqlContext.read.format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("dests.csv")
    df_dests.printSchema()
    val dests = df_dests.as[VertexId].collect().toSeq
    dests.foreach(println)



//    val result = MyShortestPaths.run(graph, dests)
//    System.out.println("result graph")
//    result.vertices.foreach(println)  // result in vertices
  }
}
