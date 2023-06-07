package com.itis.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

public class Test2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("datas/scores.txt");

        //1、切割
        //rdd = [ 语文->(80,1)，语文->(90,1),数学->(60,1),英语->(100,1),数学->(80,1）,英语->(100,1),.... ]
        JavaPairRDD<String, Tuple2<Integer, Integer>> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            public Tuple2<String, Tuple2<Integer, Integer>> call(String line) throws Exception {

                String[] arr = line.split(",");

                return new Tuple2<String, Tuple2<Integer, Integer>>(arr[1], new Tuple2<Integer, Integer>(Integer.parseInt(arr[2]), 1));
            }
        });
        //2、按照学科分组统计成绩和总个数
        //rdd = [语文 -> (500,8 ) ,数学->(300,6) ,英语->(400,10)]
        //rdd = [语文 ->  500/8,数学->300/6 ,英语->(400,10)]
        JavaPairRDD<String, Tuple2<Integer, Integer>> rdd3 = rdd2.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> agg, Tuple2<Integer, Integer> curr) throws Exception {
                return new Tuple2<Integer, Integer>(agg._1 + curr._1, agg._2 + curr._2);
            }
        });
        //3、计算平均分
        JavaPairRDD<String, Double> rdd4 = rdd3.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return new Tuple2<String, Double>(v1._1, (v1._2._1 + 0.0) / v1._2._2);
            }
        });

        System.out.println(rdd4.collect());
    }
}
