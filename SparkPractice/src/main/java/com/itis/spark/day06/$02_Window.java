package com.itis.spark.day06;

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class $02_Window {

    public static void main(String[] args) throws InterruptedException {

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
        //windowDuration： 窗口长度
        //slideDuration： 滑动长度
        //窗口长度与滑动长度必须是批次时间的整数倍
        //JavaPairDStream<String, Integer> window = ds3.window(Duration.apply(9000), Duration.apply(3000));
//
        //JavaPairDStream<String, Integer> ds5 = window.reduceByKey(new Function2<Integer, Integer, Integer>() {
        //    public Integer call(Integer v1, Integer v2) throws Exception {
        //        return v1 + v2;
        //    }
        //});

        JavaPairDStream<String, Integer> ds5 = ds3.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Duration.apply(9000), Duration.apply(3000));

        //工作中结果一般是保存到mysql、redis、mongdb、clickhouse这些写入速度比较快的地方

        ds5.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            public void call(JavaPairRDD<String, Integer> rdd, Time v2) throws Exception {

                //使用foreachpartition保存数据到mysql
               rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                   public void call(Iterator<Tuple2<String, Integer>> it) throws Exception {
                       Connection connection = null;
                       PreparedStatement statement = null;
                       try{
                           //System.out.println(s);
                           //1、获取jdbc连接
                           connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root123");
                           //2、创建statement

                           statement = connection.prepareStatement("insert into xx values(?,?)");
                           //3、sql语句赋值

                           int i = 1;
                           //遍历该分区数据
                           while (it.hasNext()){
                               Tuple2<String, Integer> element = it.next();
                               //String[] arr = element.split(",");
                               statement.setString(1,element._1);
                               statement.setInt(2,element._2);
                               //当前sql执行,添加到批次之后统一执行
                               statement.addBatch();
                               //执行一个批次[1000条数据一个批次]
                               if(i%1000==0){
                                   statement.executeBatch();
                                   statement.clearBatch();
                               }
                               i = i+1;
                           }
                           //4、执行sql
                           //执行最后一个不满1000条数据的批次
                           statement.executeBatch();
                       }catch (Exception e){

                       }finally {
                           if(statement!=null)
                               //5、关闭连接
                               statement.close();
                           if(connection!=null)
                               connection.close();
                       }

                   }
               });
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
