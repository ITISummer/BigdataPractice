package com.itis.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Iterator;

public class $05_Cache {

    /**
     * 场景:
     *      1、rdd在多个job中重复使用
     *              问题: 此时每个job启动的时候,该RDD每次都要重新计算
     *              原因: 如果将该RDD的数据保存,后续job在启动的时候不就用在重新计算该RDD的数据,直接使用保存即可。
     *      2、一个Job血统很长
     *              问题: 如果计算过程中某个RDD的分区数据丢失,需要从头计算,浪费大量时间
     *              原因: 如果将rdd的数据保存下来了,后续出错之后可以直接拿到数据重新计算得到丢失的数据,减少计算的步骤。
     *持久化方式:
     *      缓存[cache]:
     *              数据保存位置:将数据保存到内存或者是RDD所在分区的服务器本地磁盘
     *              使用方式: rdd.cache() / rdd.persist(存储级别)
     *                      cache与persist的区别:
     *                              cache是将数据保存在内存中
     *                              persist可以自由指定数据保存在内存/磁盘中
     *                       常用的存储级别:
     *                              StorageLevel.MEMORY_ONLY: 数据只保存在内存中,一般用于小数据量场景
     *                              StorageLevel.MEMORY_AND_DISK: 数据部分在内存部分在磁盘, 一般用于大数据量场景
     *              数据保存时机: 在重复使用RDD所在第一个JOB执行过程中保存
     *      checkpoint:
     *          原因: 缓存是将数据分区所在executor内存或者分区所在服务器本地磁盘,如果服务器宕机,分区数据丢失,需要根据RDD依赖重新计算得到分区数据,所以为了保证数据安全性,需要将数据保存到可靠存储介质。
     *          数据保存位置： HDFS
     *          使用方式:
     *                  1、设置数据保存位置: sc.setCheckpointDir(...)
     *                  2、保存数据: rdd.checkpoint
     *          数据保存时机: 在重复使用的RDD所在第一个job执行完成之后,会检测job中是否存在checkpoint,如果存在则会重新触发一个新的job计算得到rdd数据之后再保存。
     *          为了避免checkpoint触发的新job的数据重复计算,工作中一般会将checkpoint与cache结合使用。
     *
     * 缓存与checkpoint的区别:
     *      1、数据保存位置不一样
     *              缓存将数据保存到内存或者是RDD所在分区的服务器本地磁盘
     *              checkpoint是将数据保存到HDFS
     *      2、数据保存时机不一样
     *              缓存是在RDD所在第一个JOB执行过程中保存
     *              checkpoint是在RDD所在第一个JOB执行完成之后,单独触发一个job计算得到数据保存的
     *      3、RDD的依赖关系是否保留不一样
     *              缓存是将数据分区所在executor内存或者分区所在服务器本地磁盘,如果服务器宕机,分区数据丢失,需要根据RDD依赖重新计算得到分区数据，所以缓存是保留RDD的依赖关系
     *              checkpoint是将数据保存到HDFS,数据不会丢失，所以会切除RDD的依赖。
     *
     */
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //设置checkpoint数据保存位置
        sc.setCheckpointDir("checkpoint");


        JavaRDD<String> rdd1 = sc.textFile("datas/scores.txt");

        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] ar = line.split(",");
                System.out.println("----------->"+line);
                return new Tuple2<String, Integer>(ar[1], Integer.parseInt(ar[2]));
            }
        });

        //缓存数据
        //rdd2.cache();
        //rdd2.persist(StorageLevel.MEMORY_AND_DISK());

        //checkpoint持久化
        rdd2.checkpoint();

        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<String, Integer>(v1._1, v1._2 * 10);
            }
        });

        JavaPairRDD<String, Integer> rdd4 = rdd2.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<String, Integer>(v1._1, v1._2 * 20);
            }
        });

        System.out.println(rdd3.collect());
        System.out.println(rdd4.collect());

        System.out.println(rdd2.toDebugString());

        Thread.sleep(1000000);
    }
}
