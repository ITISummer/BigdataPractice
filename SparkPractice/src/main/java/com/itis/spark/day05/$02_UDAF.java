package com.itis.spark.day05;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;

public class $02_UDAF {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        Dataset<Row> ds = spark.read().csv("datas/scores.txt").toDF("name", "score_name", "score");

        //创建对象
        AvgAgg avgAgg = new AvgAgg();
        //转换类型【需要指定udaf参数类型的编码格式】
        UserDefinedFunction udaf = functions.udaf(avgAgg, Encoders.INT());
        //注册udaf函数
        spark.udf().register("myavg",udaf);
        ds.createOrReplaceTempView("student");

        spark.sql("select score_name,myavg(score) avg_score from student group by score_name").show();
    }
}
