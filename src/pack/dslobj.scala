package pack

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object dslobj {
  
  def main(args:Array[String]):Unit={
    
    System.setProperty("hadoop.home.dir","D:\\property")
    
    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits
   
  //  val data = spark.read.format("csv").option("header","true").load("file:///D:/data/df.csv")
   // data.show()
    
   // val dfyear = data.filter(month(col("date")) === 1).show()

    //val sel = data.select(col("date")).show()
    
   // val sel= data.selectExpr("id","tdate","amount",
             //                 "substr(category,1,3) as category",
           //                   "product","spendby",
        /////       )
   // sel.show()
   
  //  val col = data.withColumn("Status",expr("case when spendby = 'cash' then 1 else 0 end as status"))
     //             .withColumn("NewAmount", expr("amount+2000"))
     //             .withColumn("amount",expr("round(amount)"))
   // col.show()
    
    val sor = spark.read.format("csv").option("header","true").load("file:///D:/data/source.csv")
   // sor.show()
    val tar = spark.read.format("csv").option("header","true").load("file:///D:/data/target.csv")
    //tar.show()
    
    val join1 = sor.join( tar , Seq("id") ,"full").orderBy("id")
    //join1.show()
    
    val col = join1.withColumn("comment", expr("case when sname=tname then 'match' else 'mismatch' end"))
    //col.show()
    
    val filter = col.filter(!(col("comment")==="match"))
    //filter.show()
    
    val finaldf = filter.withColumn("comment",expr("case when sname is Null then 'new in target' when tname is Null then 'new in source' else comment end"))
  //  finaldf.show()
    
    val output = finaldf.select("id","comment")
    output.show()
    

println("work done")
  }
}