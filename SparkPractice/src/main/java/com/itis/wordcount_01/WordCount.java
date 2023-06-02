package com.itis.wordcount_01;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;
import org.apache.spark.SparkConf;

import java.util.Arrays;

public class WordCount {

    @Test
    public void run() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").setAppName("SparkSolution");
        JavaSparkContext context = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
        JavaRDD<String> source = context.textFile("input/words.txt");
        source.flatMap(str -> Arrays.stream(str.split(" ")).iterator())
                .mapToPair(str -> new Tuple2<>(str, 1))
                .reduceByKey(Integer::sum)
                .saveAsTextFile("output/practice");
        context.close();
    }
}
