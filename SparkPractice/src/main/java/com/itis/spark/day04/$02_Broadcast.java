package com.itis.spark.day04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class $02_Broadcast {

    /**
     * 广播变量的使用场景
     *      1、如果task中使用到driver的数据并且该数据有一定大小
     *              问题: driver会将该数据传递给所有的task，占用的内存大小 = task个数 * 数据大小,最终导致占用的内存会比较大
     *              广播之后:占用的内存 = executor个数 * 数据大小，减少内存空间的占用
     *      2、大表 join 小表
     *              问题: join会产生shuffle
     *              此时可以将小表的数据通过collect收集到driver端之后广播给executor,后续实现map join.避免shuffle操作。
     * 使用:
     *      1、广播数据: BroadCast bc = sc.broadcast(数据)
     *      2、task使用广播数据: 在call方法中直接通过 bc.value 取出数据使用
     *
     */
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("atguigu", "spark", "hadoop", "flume", "kafka"));


        final Map<String,String> nt  = new HashMap<String, String>();
        nt.put("atguigu","www.atguigu.com");
        nt.put("spark","spark.apache.org");
        nt.put("hadoop","hadoop.apache.org");
        nt.put("flume","flume.apache.org");
        nt.put("kafka","kafka.apache.org");

        //广播数据
        final Broadcast<Map<String, String>> bc = sc.broadcast(nt);
        JavaRDD<String> rdd2 = rdd1.map(new Function<String, String>() {
            public String call(String v1) throws Exception {

                //取出广播数据
               Map<String, String> map = bc.value();
                return map.get(v1);
            }
        });

        System.out.println(rdd2.collect());

        Thread.sleep(1000000);
    }
}
