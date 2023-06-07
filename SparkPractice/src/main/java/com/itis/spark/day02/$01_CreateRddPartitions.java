package com.itis.spark.day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class $01_CreateRddPartitions implements Serializable {


    /**
     * 通过集合创建的RDD的分区数
     *      1、如果创建RDD的时候指定了numSlices参数,此时RDD的分区数 = numSlices
     *      2、如果创建RDD的时候没有指定numSlices参数,此时RDD的分区数 = defaultParallelism
     *              1、如果在sparkconf中指定了spark.default.parallelism,此时RDD的分区数 = spark.default.parallelism参数值
     *              2、如果在sparkconf中没有指定spark.default.parallelism
     *                      1、master=local,此时RDD的分区数 = 1
     *                      2、master=local[N],此时RDD的分区数 = N
     *                      3、master=local[*],此时RDD的分区数 = cpu个数
     *                      4、master=yarn, 此时RDD的分区数 = max(所有executor cpu总核数,2)
     */
    @Test
    public void createRddByCollectionPartitions(){

        SparkConf conf = new SparkConf()
                //.set("spark.default.parallelism","10")
                .setMaster("local[3]")
                .setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //集合的切片规划: 第N个分区处理集合角标范围: [ N * 元素个数 / 分区数 , (N+1)*元素个数 /分区数 ）  的数据。
        //JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 5, 2, 6, 8, 9),3);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 5, 2, 6, 8, 9,23));


        JavaRDD<Integer> rdd2 = rdd1.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            public Iterator<Integer> call(Iterator<Integer> it) throws Exception {

                while (it.hasNext()) {
                    System.out.println(Thread.currentThread().getName() + "----->" + it.next());
                }
                return it;
            }
        });

        rdd2.collect();
        //查看分区数
        System.out.println(rdd1.getNumPartitions());
    }

    /**
     * 通过读取文件创建的RDD的分区数
     *      1、如果创建RDD的时候指定了minPartitions参数,此时RDD的分区数 >= minPartitions
     *      2、如果创建RDD的时候没有指定minPartitions参数,此时RDD的分区数 >= min( defaultParallelism,2 )
     * 读取文件创建的RDD的分区数最终是取决于文件的切片数。
     */
    @Test
    public void createRddByFilePartitions(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("datas/wc.txt",4);

        System.out.println(rdd1.getNumPartitions());
    }

    /**
     * 通过其他RDD衍生出新RDD的分区数 = 父RDD的分区数
     */
    @Test
    public void createRddByRddPartitions(){
        SparkConf conf = new SparkConf()
                //.set("spark.default.parallelism","10")
                .setMaster("local[4]")
                .setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 5, 2, 6, 8, 9),3);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 5, 2, 6, 8, 9));

        JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 10;
            }
        });

        System.out.println(rdd2.getNumPartitions());
    }
}
