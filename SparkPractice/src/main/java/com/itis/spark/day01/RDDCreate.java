package com.itis.spark.day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RDDCreate {


    /**
     * RDD的创建方式:
     *      1、通过集合创建
     *      2、通过文件创建
     *      3、通过其他的RDD衍生
     */

    /**
     * 通过集合创建RDD
     */
    @Test
    public void createRddByCollection(){

        //1、创建spark程序的入口: JavaSparkContext
        //setMaster在idea执行的时候必须配置
        //setMaster如果后续提交到集群执行,不要配置,在spark-submit --master配置
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,46,7,8,9));

        List<Integer> result = rdd1.collect();

        for(Integer i: result){
            System.out.println("i="+i);
        }
    }

    /**
     * 2、通过文件创建
     */
    @Test
    public void createRddByFile(){
        System.setProperty("HADOOP_USER_NAME","atguigu");
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("hdfs://hadoop102:8020/sparklog");

        List<String> result = rdd1.collect();

        for(String s: result){
            System.out.println(s);
        }
    }

    /**
     * 3、其他RDD衍生
     */
    @Test
    public void createRddByRDD(){
        System.setProperty("HADOOP_USER_NAME","atguigu");
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("datas/wc.txt");

        JavaRDD<String> rdd2 = rdd1.distinct();

        List<String> result = rdd2.collect();

        for(String s: result){
            System.out.println(s);
        }
    }
}
