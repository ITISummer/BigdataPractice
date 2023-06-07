package com.itis.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class $01_Actions implements Serializable {


    /**
     * collect: 用于收集RDD分区的数据以集合的形式封装之后交给Driver
     *    如果RDD数据量比较大,Drvier默认的内存只有1G,此时数据放不下会出现内存溢出。
     *    所以在工作中，Driver的内存一般需要设置,一般设置为5-10G
     *    collect后续一般用于广播变量
     */
    @Test
    public void collect(){


        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 4, 234, 6, 56, 8));

        List<Integer> result = rdd.collect();

        System.out.println(result);
    }

    /**
     * count: 统计RDD元素个数
     * first: 获取RDD第一个元素
     * take: 获取前N个元素
     */
    @Test
    public void others() throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 4, 234, 6, 56, 8));

        System.out.println(rdd.count());

        System.out.println(rdd.first());

        System.out.println(rdd.take(3));

    }

    /**
     * 统计每个key的个数
     * countByKey一般用于数据倾斜场景,用于发现哪个key导致的数据倾斜
     */
    @Test
    public void countByKey(){
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("aa,1", "bb,2", "aa,10", "cc,23", "aa,50", "bb,22"));

        JavaPairRDD<String, Integer> rdd2 = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<String, Integer>(arr[0], Integer.parseInt(arr[1]));
            }
        });

        Map<String, Long> map = rdd2.countByKey();

        System.out.println(map);

    }

    @Test
    public void save(){
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("aa,1", "bb,2", "aa,10", "cc,23", "aa,50", "bb,22"));

        rdd.saveAsTextFile("output");
    }

    /**
     * foreach: 遍历RDD元素
     */
    @Test
    public void foreach(){
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("aa,1", "bb,2", "aa,10", "cc,23", "aa,50", "bb,22"));

        //foreach是针对每个元素处理,当我使用foreach保存数据到mysql的时候,每个元素都需要打开连接、关闭连接,当数据量比较大的时候,会影响性能。
        //spark算子中Function里面的call方法的逻辑是在task执行, spark算子外面的代码是在driver执行的
        rdd.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {

                Connection connection = null;
                PreparedStatement statement = null;
                try{
                    //System.out.println(s);
                    //1、获取jdbc连接
                    connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root123");
                    //2、创建statement
                    String[] arr = s.split(",");
                    statement = connection.prepareStatement("insert into xx values(?,?)");
                    //3、sql语句赋值
                    statement.setString(1,arr[0]);
                    statement.setInt(2,Integer.parseInt(arr[1]));
                    //4、执行sql
                    statement.executeUpdate();
                }catch (Exception e){

                }finally {
                    if(statement!=null)
                        //5、关闭连接
                        statement.close();
                    if(connection!=null)
                        connection.close();
                }



            }
        });
    }

    /**
     * foreachPartition: 针对每个分区进行处理
     * foreach: 是针对每个分区的每条数据处理
     * foreachPartition的使用场景:一般用于保存数据且获取外部资源链接的场景,可以减少链接创建和销毁的次数
     */
    @Test
    public void foreachPartition(){
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("aa,1", "bb,2", "aa,10", "cc,23", "aa,50", "bb,22"));

        //Iterator<String>: 封装的一个分区所有的数据
        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            public void call(Iterator<String> it) throws Exception {

                Connection connection = null;
                PreparedStatement statement = null;
                try{
                    //System.out.println(s);
                    //1、获取jdbc连接
                    connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "root123");
                    //2、创建statement

                    statement = connection.prepareStatement("insert into xx values(?,?)");
                    //3、sql语句赋值

                    int i = 1;
                    //遍历该分区数据
                    while (it.hasNext()){
                        String element = it.next();
                        String[] arr = element.split(",");
                        statement.setString(1,arr[0]);
                        statement.setInt(2,Integer.parseInt(arr[1]));
                        //当前sql执行,添加到批次之后统一执行
                        statement.addBatch();
                        //执行一个批次[1000条数据一个批次]
                        if(i%1000==0){
                            statement.executeBatch();
                            statement.clearBatch();
                        }
                        i = i+1;
                    }
                    //4、执行sql
                    //执行最后一个不满1000条数据的批次
                    statement.executeBatch();
                }catch (Exception e){

                }finally {
                    if(statement!=null)
                        //5、关闭连接
                        statement.close();
                    if(connection!=null)
                        connection.close();
                }


            }
        });
    }
}
