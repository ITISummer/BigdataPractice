package com.itis.spark.day06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class $03_Close {

    public static void main(String[] args) throws InterruptedException, URISyntaxException, IOException {
        JavaStreamingContext ssc = new JavaStreamingContext(new SparkConf().setMaster("local[4]").setAppName("test"), Duration.apply(3000));
        ssc.sparkContext().setLogLevel("error");
        JavaReceiverInputDStream<String> ds1 = ssc.socketTextStream("hadoop102", 9999);

        JavaDStream<String> ds2 = ds1.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] arr = s.split(" ");
                List<String> list = Arrays.asList(arr);
                return list.iterator();
            }
        });

        JavaPairDStream<String, Integer> ds3 = ds2.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<String, Integer>(s, 1);
            }
        });

        ds3.cache();

        JavaPairDStream<String, Integer> ds31 = ds3.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<String, Integer>("---->" + v1._1, v1._2);
            }
        });

        ds31.print();


        ssc.start();

        //停止条件最好是通过外部触发的方式：
        //   1、一直监听HDFS某个目录,如果目录不存在了,停止
        //   2、一直监听mysql某个表某个字段的值,如果值变化了,停止

        URI uri = new URI("hdfs://hadoop102:8020");
        FileSystem fs = FileSystem.get(uri, new Configuration());

        while( fs.exists(new Path("hdfs://hadoop102:8020/input")) ){

            Thread.sleep(5000);
        }
        //stopGracefully = true 代表将接收到批次数据处理完成之后停止
        ssc.stop(true,true);

        //ssc.awaitTermination();

    }
}
