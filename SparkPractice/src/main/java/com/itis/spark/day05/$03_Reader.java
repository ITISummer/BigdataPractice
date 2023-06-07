package com.itis.spark.day05;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Properties;

public class $03_Reader {

    /**
     * sparksql读取数据有两种方式:
     *      1、spark.read()
     *          .format("csv/text/jdbc/parquet/orc/json") --指定读取数据格式
     *          [.option("..","..")... ] --指定读取数据需要的参数
     *          .load([path])  [一般不用]
     *      2、spark.read()[.option(..,..)...].csv/json/parquet/orc/jdbc  [常用]
     */

    /**
     * 读取文件
     */
    @Test
    public void readFile(){

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        //读取文本文件
        //第一种方式
        spark.read().format("text").load("datas/wc.txt");//.show();

        //第二种方式
        spark.read().text("datas/wc.txt");//.show();

        //读取json数据
        spark.read().json("datas/js.json");//.show();

        //读取csv文件
        //csv常用的option:
        //      sep: 指定列之间的分隔符
        //      header: 是否以文件的第一行作为列名
        //      inferSchema: 是否自动推断列的类型
        spark.read().option("header","true").csv("datas/mycsv.csv");//.show();

        Dataset<Row> df = spark.read()
                .option("sep","\t")
                .option("inferSchema","true")
                .csv("datas/product.txt");
        //df.printSchema();

        //读取paruqet
        //spark.read().parquet("...");

        //spark.read().orc("...");
    }


    @Test
    public void readMysql(){

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        //指定mysql的url
        String url ="jdbc:mysql://hadoop102:3306/gmall";

        //指定读取的表
        //读取整表所有数据
        String tableName = "user_info";
        //读取指定数据
        //String tableName = "(select * from user_info where id<50) tmp";

        //指定连接参数
        Properties props = new Properties();
        props.setProperty("user","root");
        props.setProperty("password","root123");

        //TODO 第一种读取mysql方式
        //TODO 此种方式读取Mysql只有一个分区,此时方式只适合读取小数据量场景
        Dataset<Row> df = spark.read().jdbc(url, tableName, props);
        //System.out.println(df.javaRDD().getNumPartitions());

        //TODO 第二种读取mysql方式
        //TODO conditions数组里面的每个元素代表每个分区拉取数据的where条件
        //TODO 此种方式读取mysql的分区数 = conditions中元素的个数
        //TODO 不常用
        String[] conditions = {"id<=10","id>10 and id<=20","id>20 and id<=40","id>40"};
        Dataset<Row> df2 = spark.read().jdbc(url, tableName, conditions, props);
        //System.out.println(df2.javaRDD().getNumPartitions());

        //TODO 第三种读取mysql方式
        //TODO columName: 用于分区的mysql的字段名，要求必须是数字、日期、时间戳的列
        //TODO lowerBound与upperBound是用于决定分区跨距，一般用查询数据集的最小值和最大值
        //TODO 此种方式分区数: (upperBound-lowerBound)<numPartitions ?  (upperBound-lowerBound) : numPartitions
        //TODO 此种方式一般用于大数据量场景

        //动态获取lowerbound与uppwerbound的值
        Dataset<Row> df5 = spark.read().jdbc(url, "(select min(id) min_id,max(id) max_id from user_info) user_info", props);

        Row row = df5.first();
        //row类型取值: (列的类型)row.getAs("列名")
        Long min_id = (Long) row.getAs("min_id");
        Long max_id = (Long) row.getAs("max_id");
        String columnName ="id";
        Long lowerBound = min_id;
        Long upperBound = max_id;

        Dataset<Row> df3 = spark.read().jdbc(url, tableName, columnName, lowerBound, upperBound, 10, props);
        //df3.show();
        System.out.println(df3.javaRDD().getNumPartitions());
        df3.javaRDD().saveAsTextFile("output");
    }
}
