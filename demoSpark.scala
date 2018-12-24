package scala.main

import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.io.Source;

object demoSpark {
  def main(args: Array[String]): Unit = {
     val data=Array(1,2,3,4,5,6,7,8,9,0);
     val conf=new SparkConf().setAppName("FirstApp").setMaster("local");
     val sc=new SparkContext(conf);
     val distData=sc.parallelize(data,3);
     distData.foreach(println);

     /*
        Entry point into all functionality in Spark is the SparkSession class. To create a Spark session below
        definition can be used.
        After Spark 2.0 version , Spark provides built-in support for Hive features including the ability to write queries using HiveQL, access to Hive UDFs and
        the ability to read data from Hive tables. So, no need to have an existing Hive Setup.
      */
     val sparkSession=SparkSession.builder().appName("SQL APP").master("local").config("spark.hadoop.hive.metastore.warehouse.dir","C:\\Users\\aWX614523\\AppData\\Local\\Temp\\").enableHiveSupport().getOrCreate();
     import sparkSession.implicits._;
     println("Version of Spark : "+sparkSession.version);
     /*
       sparkContext can be access using sparkSession too as sparkSession is the entry point of Spark programming
      */
      val distData1=sparkSession.sparkContext.parallelize(data,4);
      distData1.foreach(println);
      val dbResult=sparkSession.sql("show databases");
      println(dbResult.show());
      /*
          Read customized query file and executes SQL queries sequentially.
       */
       val fileName="D:\\SparkTutorialIntellij\\resources\\query.sql";
       for(line<-Source.fromFile(fileName).mkString.split("\n")){
           println(line);
           sparkSession.sql(line).show();
       }
    /*
       With a sparkSession, applications can create DataFrames from an existing RDD, from a Hive table or from Spark Data sources like
                                                                                                               parquet
                                                                                                               orc
                                                                                                               json
                                                                                                               hive tables
                                                                                                               JDBC to other databases
                                                                                                               Avro files
       Below creates a dataframe based on the json as input file
     */
      val df=sparkSession.read.json("D:\\SparkTutorialIntellij\\resources\\input.json");
      df.show();
    /*
       Print the schema in a tree format
     */
    df.printSchema();
    /*
        Select only one column(Age)
     */
    df.select("Age").show();

    /*
        Select "Age" column and increment the value by 1
    */
    df.select($"Age",$"Time"+1).show();


  }

}
