package com.itis.spark.day05;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class $06_Cover {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        Dataset<Row> df = spark.read().text("datas/wc.txt");
        //DataFrame转rdd
        JavaRDD<Row> rdd = df.javaRDD();

        //DataFrame转DataSet
        Dataset<String> ds = df.as(Encoders.STRING());

        ds.show();

        //DataSet转DataFrame
        Dataset<Row> df2 = ds.toDF("line");

        //DataSet转rdd
        JavaRDD<String> rdd2 = ds.javaRDD();

        //rdd转dataFrame
        //Dataset<Row> df4 = spark.createDataFrame(rdd2,String.class);
        //df4.show();

    }
}
