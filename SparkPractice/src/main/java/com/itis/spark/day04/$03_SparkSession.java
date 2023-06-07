package com.itis.spark.day04;

import org.apache.spark.sql.SparkSession;

public class $03_SparkSession {

    public static void main(String[] args) {

        //sparksql的入口
        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();
    }
}
