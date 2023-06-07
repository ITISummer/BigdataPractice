package com.itis.spark.day05;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

public class $04_Writer {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();


        /**
         *  写数据的方式有两种:
         *       1、ds.write()
         *              .mode(SaveMode.XXX) --指定写入模式
         *              .format("csv/json/text/parquet/orc/jdbc") --保存数据的格式
         *              [.option("..","...")....]
         *              .save([path])
         *      2、ds.write().mode(SaveMode.XXX)[.option("..","...")....].csv/json/text/paruqet/orc/jdbc [常用]
         *
         * 常用的写入模式
         *         SaveMode.Append: 如果保存数据的目录/表已经存在,则追加数据
         *         SaveMode.Overwrite: 如果保存数据的目录/表已经存在,则覆盖数据 [一般用于写入数据到HDFS]
         */

        Dataset<Row> ds = spark.read().csv("datas/scores.txt").toDF("id", "name", "score_name", "score");

        //保存为文本
        //数据集只有一个列才能保存为文本
        //ds.write().text("output/txt");

        //保存为json
        //ds.write().mode(SaveMode.Overwrite).json("ouput/json");

        //保存为csv
        //ds.write().option("sep","\t").option("header","true").mode(SaveMode.Overwrite).csv("output/csv");

        //保存数据到mysql
        String url ="jdbc:mysql://hadoop102:3306/test";
        String table = "test";
        Properties props = new Properties();
        props.setProperty("user","root");
        props.setProperty("password","root123");
        //ds.write().mode(SaveMode.Append).jdbc(url,table,props);
        //工作中保存数据到MYSQL都是使用foreachPartition写入,防止主键冲突问题
        ds.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            public void call(Iterator<Row> it) throws Exception {

                Connection connection = null;
                PreparedStatement statement = null;
                try{

                    connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","root123");

                    statement = connection.prepareStatement("INSERT INTO test VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE `name`=?,score_name=?,score=?");

                    int i = 1;
                    while (it.hasNext()){

                        Row row = it.next();
                        String id = (String) row.getAs("id");
                        String name = (String) row.getAs("name");
                        String score_name = (String) row.getAs("score_name");
                        String score = (String) row.getAs("score");
                        statement.setString(1,id);
                        statement.setString(2,name);
                        statement.setString(3,score_name);
                        statement.setString(4,score);
                        statement.setString(5,name);
                        statement.setString(6,score_name);
                        statement.setString(7,score);

                        statement.addBatch();

                        if(i%1000==0){
                            statement.executeBatch();
                            statement.clearBatch();
                        }

                        i = i + 1;

                    }

                    statement.executeBatch();

                }catch (Exception e){

                }finally {

                    if(statement!=null)
                        statement.close();
                    if(connection!=null)
                        connection.close();
                }

            }
        });
    }
}
