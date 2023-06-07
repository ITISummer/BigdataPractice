package com.itis.spark.day04;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        //1、读取数据
        JavaRDD<String> rdd1 = sc.textFile("datas/user_visit_action.txt",4);

        //2、过滤掉搜索行为数据
        JavaRDD<String> rdd2 = rdd1.filter(new Function<String, Boolean>() {
            public Boolean call(String line) throws Exception {
                String[] arr = line.split("_");
                String searchKeyword = arr[5];
                return searchKeyword.equals("null");
            }
        });
        //3、列裁剪[只需要点击品类、下单品类ids、支付品类ids]
        //[
        //   (1, null,null ),
        //   (-1, "2,3,4" ,null ),
        //   (-1, null, "4,5,6" ),
        //   (-1, null, "4,7,8" )
        // ]
        JavaRDD<Tuple3<String, String, String>> rdd3 = rdd2.map(new Function<String, Tuple3<String, String, String>>() {
            public Tuple3<String, String, String> call(String line) throws Exception {
                String[] arr = line.split("_");
                String clickId = arr[6];
                String orderIds = arr[8];
                String payids = arr[10];
                return new Tuple3<String, String, String>(clickId, orderIds, payids);
            }
        });


        //3、切割、炸裂、转换数据类型
        //[
        //  (1, (1,0,0) ),
        //  (2, (0,1,0)),
        //  (3, (0,1,0)),
        //  (4, (0,1,0)),
        //  (4, (0,0,1)),
        //  (5, (0,0,1)),
        //  (6, (0,0,1)),
        //  (4, (0,0,1)),
        //  (7, (0,0,1)),
        //  (8, (0,0,1))
        // ]

        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rdd4 = rdd3.flatMapToPair(new PairFlatMapFunction<Tuple3<String, String, String>, String, Tuple3<Integer, Integer, Integer>>() {
            public Iterator<Tuple2<String, Tuple3<Integer, Integer, Integer>>> call(Tuple3<String, String, String> v1) throws Exception {
                //v1 = (-1, "2,3,4" ,null )
                String clickid = v1._1();
                String orderIds = v1._2();
                String payIds = v1._3();

                ArrayList<Tuple2<String, Tuple3<Integer, Integer, Integer>>> res = new ArrayList<Tuple2<String, Tuple3<Integer, Integer, Integer>>>();

                if (!clickid.equals("-1")) {
                    //代表当前这条数据是点击行为数据

                    res.add(new Tuple2<String, Tuple3<Integer, Integer, Integer>>(clickid, new Tuple3<Integer, Integer, Integer>(1, 0, 0)));
                } else if (!orderIds.equals("null")) {
                    //下单行为
                    String[] arr = orderIds.split(",");

                    for (String orderId : arr) {

                        res.add(new Tuple2<String, Tuple3<Integer, Integer, Integer>>(orderId, new Tuple3<Integer, Integer, Integer>(0, 1, 0)));
                    }
                } else {
                    //支付行为
                    String[] arr = payIds.split(",");

                    for (String payId : arr) {

                        res.add(new Tuple2<String, Tuple3<Integer, Integer, Integer>>(payId, new Tuple3<Integer, Integer, Integer>(0, 0, 1)));
                    }
                }

                return res.iterator();
            }
        });


        //4、按照品类分组,统计点击次数、下单次数、支付次数
        //[
        // (1,(1,0,0))
        // (2,(5,3,1))
        // (3,(15,10,7))
        // ...
        // ]
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> rdd5 = rdd4.reduceByKey(new Function2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            public Tuple3<Integer, Integer, Integer> call(Tuple3<Integer, Integer, Integer> v1, Tuple3<Integer, Integer, Integer> v2) throws Exception {
                //v1 = (1,0,0)   v2 = (0,1,0)

                return new Tuple3<Integer, Integer, Integer>(v1._1() + v2._1(), v1._2() + v2._2(), v1._3() + v2._3());
            }
        });

        //5、按照次数排序取前十
        JavaRDD<UserActions> rdd6 = rdd5.map(new Function<Tuple2<String, Tuple3<Integer, Integer, Integer>>, UserActions>() {

            public UserActions call(Tuple2<String, Tuple3<Integer, Integer, Integer>> v1) throws Exception {
                UserActions action = new UserActions();
                action.setId(v1._1());
                action.setClickNum(v1._2()._1());
                action.setOrderNum(v1._2()._2());
                action.setPayNum(v1._2()._3());
                return action;
            }
        });


        JavaRDD<UserActions> rdd7 = rdd6.sortBy(new Function<UserActions, UserActions>() {
            public UserActions call(UserActions v1) throws Exception {
                return v1;
            }
        }, false, 4);


        List<UserActions> list = rdd7.take(10);

        for(UserActions us : list){

            System.out.println(us);
        }
    }
}
