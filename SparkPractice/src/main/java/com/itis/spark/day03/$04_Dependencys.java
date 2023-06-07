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

public class $04_Dependencys {

    /**
     * RDD依赖分为两种:
     *      宽依赖: 有shuffle的称之为宽依赖【父RDD一个分区的数据被子RDD多个分区使用】
     *      窄依赖: 没有shuffle的就是窄依赖 【父RDD一个分区的数据被子RDD一个分区使用】
     *
     * Application: 应用[一个sparkcontext就是一个应用]
     *      Job: 任务[一般一个action算子产生一个job]
     *          stage: 阶段 [ 一个job中stage个数 = shuffle个数+1 ]
     *              task: 子任务[ 一个stage中task个数 = 该stage种最后一个rdd的分区数]
     *
     *一个job中多个stage是串行的，先执行前面的stage然后执行后面的stage
     *一个stage中多个task是并行的
     *
     * stage的切分: 根据job最后一个rdd的依赖关系从后往前依次查找,一致查询到第一个rdd为止,在查询的过程中会判断父子RDD的依赖是否为宽依赖,如果是则切分stage
     *
     */
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //1、读取数据
        //rdd= [hello spark hello,hadoop flume kafka,hadoop spark spark,flume flume kafka,flume flume kafka]
        JavaRDD<String> rdd1 = sc.textFile("datas/wc.txt");

        System.out.println("++++++++++++++++++++++++++++++++++++++++++");
        System.out.println(rdd1);
        System.out.println("-------------------------------------------");
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
        System.out.println("-------------------------------------------");
        System.out.println(rdd2.toDebugString());
        System.out.println("-------------------------------------------");
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
        System.out.println("-------------------------------------------");
        System.out.println(rdd3.toDebugString());
        System.out.println("-------------------------------------------");
        //4、结果打印
        System.out.println(rdd3.collect());

        rdd3.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t1) throws Exception {
                System.out.println(t1);
            }
        });

        Thread.sleep(100000);
    }
}
