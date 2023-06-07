package com.itis.spark.day04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class $01_Partitioner {

    /**
     * spark在shuffle的时候需要用到partitioner, spark的partitioner有两种：
     *          HashPartitioenr:
     *              分区规则: key.hashCode % 分区数 < 0 ? key.hashCode % 分区数 + 分区数 : key.hashCode % 分区数
     *          RangePartitioner:
     *               分区规则:
     *                  1、根据抽样算法,抽取N个key,
     *                  2、此时根据抽取的N个key,排序之后,平均分为M段[M个分区],后续每个分区确定一个边界值
     *                  3、后续数据根据key与每个分区的边界值对比,是否在边界值范围内,如果是则放入分区中
     *
     */
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);


    }
}
