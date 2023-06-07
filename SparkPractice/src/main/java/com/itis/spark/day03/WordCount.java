package com.itis.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()/*.setMaster("local[4]")*/.setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);


            //1、读取数据
            //rdd= [hello spark hello,hadoop flume kafka,hadoop spark spark,flume flume kafka,flume flume kafka]
            JavaRDD<String> rdd1 = sc.textFile(args[0]);

            //2、切割转换数据类型 :单词->1
            //rdd2 = [hello->1,spark->1,hello->1,hadoop->1,flume->1,....]
            JavaPairRDD<String, Integer> rdd2 = rdd1.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
                public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {
                    //line = hello spark hello
                    List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();
                    String[] arr = line.split(" ");
                    for (String wc : arr) {
                        result.add(new Tuple2<String, Integer>(wc, 1));
                    }

                    return result.iterator();
                }
            });

            //3、按照单词分组,统计次数
            //[
            //   hello -> [1,1,1,1,1]
            //   spark -> [1,1,1,1]
            // ]
            JavaPairRDD<String, Integer> rdd3 = rdd2.reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });

            //4、结果打印
            System.out.println(rdd3.collect());

            rdd3.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                public void call(Tuple2<String, Integer> t1) throws Exception {
                    System.out.println(t1);
                }
            });




    }
}
