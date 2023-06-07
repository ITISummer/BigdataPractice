package com.itis.spark.day06;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class $01_WordCount {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("test");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Duration.apply(5000));
        ssc.sparkContext().setLogLevel("error");

        Map<String,Object> props = new HashMap<String,Object>();
        //指定key的反序列化器
        props.put("key.deserializer", StringDeserializer.class);
        //指定value的反序列化器
        props.put("value.deserializer", StringDeserializer.class);
        //指定kafka 集群地址
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092");
        //指定消费者组的id
        props.put("group.id", "spark02");
        //指定消费者组第一次消费topic的时候从哪个位置开始消费
        props.put("auto.offset.reset", "earliest");
        //指定消费的topic的名称
        Collection<String> topics = Arrays.asList("spark_log");

        JavaInputDStream<ConsumerRecord<String, String>> ds = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, props));

        ds.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd1, Time v2) throws Exception {

                System.out.println("------------->:"+rdd1.getNumPartitions());
                JavaRDD<String> rdd2 = rdd1.map(new Function<ConsumerRecord<String, String>, String>() {
                    public String call(ConsumerRecord<String, String> record) throws Exception {

                        return record.value();
                    }
                });
                //rdd = ["hello spark","hello java","spark hadoop hive"]
                JavaRDD<String> rdd3 = rdd2.flatMap(new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String line) throws Exception {
                        String[] arr = line.split(" ");
                        List<String> list = Arrays.asList(arr);
                        return list.iterator();
                    }
                });
                //rdd3 = [hello,spark,hello,java,spar,hadoop,hive]

                JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
                //rdd4 = [hello->1,spark->1,hello->1,java->1,....]
                JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

                rdd5.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    public void call(Tuple2<String, Integer> v1) throws Exception {
                        System.out.println(v1);
                    }
                });
            }
        });

        //启动程序
        ssc.start();
        //阻塞,等待外部停止程序
        ssc.awaitTermination();
    }
}
