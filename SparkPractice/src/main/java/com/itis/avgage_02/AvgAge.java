package com.itis.avgage_02;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;


public class AvgAge {
    @Test
    public void run() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("SparkSolution");
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        JavaRDD<String> rdd = sc.textFile("input/age.txt");
        rdd.mapToPair(line -> {
            String[] split = line.split("_");
            return new Tuple2<>(split[0], new Tuple2<>(Integer.valueOf(split[2]), 1L));
        })
//                new Tuple2<>(str.split("_")[0], new Tuple3<>(str.split("_")[0], Integer.parseInt(str.split("_")[2]), 1)))
                .reduceByKey((v1,v2)-> new Tuple2<>(v1._1+v2._1,v1._2+v2._2))
                .mapValues(val->String.format("%.2f",val._1.doubleValue()/val._2))
//                .saveAsTextFile("output/practice");
                .collect()
                .forEach(System.out::println);
        sc.close();
    }
}
