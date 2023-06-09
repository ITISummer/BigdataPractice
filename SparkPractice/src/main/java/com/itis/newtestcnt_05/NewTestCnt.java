package com.itis.newtestcnt_05;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NewTestCnt {

    @Test
    public void run() {

        SparkConf conf = new SparkConf().setAppName("SparkSolution").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("input/score.csv")
                .mapToPair(v1 -> {
                    String[] splits = v1.split(",");
                    return new Tuple2<>(splits[1], splits[0]);
                }).groupByKey()
                .mapToPair(v1 -> {
                    List<String> timeList = new ArrayList<>();
                    v1._2.forEach(timeList::add);
                    return new Tuple2<>(Collections.min(timeList), 1);
                })
                .reduceByKey(Integer::sum)
                .map(v1 -> v1._1+","+v1._2)
//                .collect()
//                .forEach(System.out::println);
              .saveAsTextFile("output/practice");

        sc.close();
    }

}
