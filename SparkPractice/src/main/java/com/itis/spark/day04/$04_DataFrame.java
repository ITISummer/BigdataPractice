package com.itis.spark.day04;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class $04_DataFrame {

    /**
     * sparksql的编程有两种方式:
     *      声明式: 使用sql语句操作数据
     *              1、将数据集注册成表: createOrReplaceTempView("表名")
     *              2、写sql操作: spark.sql("sql语句")
     *      命令式: 使用api带名操作数据[了解]
     *
     */
    public static void main1(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        //读取数据
        Dataset<String> df = spark.read().textFile("datas/wc.txt");

        //取别名
        df.createOrReplaceTempView("wordcount");

        spark.sql("select wc,count(1) num from wordcount lateral view explode(split(value,' ')) tmp as wc group by wc").createOrReplaceTempView("tmp_1");

        spark.sql("select * from tmp_1").show();





    }


    /**
     * 命令式
     * @param args
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        //读取数据
        Dataset<Row> df = spark.read().option("sep", "\t").csv("datas/product.txt")
                //重定义列名
                .toDF("name","price","dt","market","province","city");
        //过滤
        //df.where("province='山西'").show();
        //df.filter("province='山西'").show();

        //列裁剪
        //df.select("name","province").show();
        //df.selectExpr("avg(price) as avg_price").show();

        //去重
        //df.distinct();

        //分组、聚合
        df.selectExpr("province","cast(price as decimal(16,2)) price").groupBy("province").avg("price").show();
    }
}
