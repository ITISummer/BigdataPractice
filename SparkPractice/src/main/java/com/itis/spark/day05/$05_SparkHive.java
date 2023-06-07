package com.itis.spark.day05;

import org.apache.spark.sql.SparkSession;

public class $05_SparkHive {

    /**
     * idea spark整合hive
     *      1、加入spark-hive的依赖
     *      2、将hive-site.xml放入resource目录
     *      3、在创建sparksession对象的时候开启hive支持: enableHiveSupport()
     *      4、读取hive表: spark.sql("....")
     *      5、保存数据到hive: spark.sql("load data / insert into /insert overwrite ....")
     */
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","atguigu");
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("test")
                .enableHiveSupport()
                .getOrCreate();

        //spark.sql("show tables").show();

        //查询hive表
        //spark.sql("select * from user_info").show();

        //写入hive表
        spark.sql("insert into user_info select * from user_info");

    }
}
