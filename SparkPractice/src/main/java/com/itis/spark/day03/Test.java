package com.itis.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Test {

    /**
     * 统计农产品种类数最多的三个省份
     */
    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //1、读取数据
        //rdd=[ "香菜	2.80	2018/1/1	山西汾阳市晋阳农副产品批发市场	山西	汾阳"，"大葱	2.80	2018/1/1	山西汾阳市晋阳农副产品批发市场	山西	汾阳"]
        JavaRDD<String> rdd1 = sc.textFile("datas/product.txt");

        //要不要过滤、要不要去重、要不要列裁剪
        //1.1、过滤脏数据
        JavaRDD<String> rdd11 = rdd1.filter(new Function<String, Boolean>() {
            public Boolean call(String line) throws Exception {
                return line.split("\t").length == 6;
            }
        });

        //2、切割、列裁剪
        //rdd=[ (山西,香菜),(山西,大白菜),(湖南,上海青),(山西,香菜) ,.....]
        JavaRDD<Tuple2<String, String>> rdd2 = rdd11.map(new Function<String, Tuple2<String, String>>() {
            public Tuple2<String, String> call(String line) throws Exception {

                    String[] arr = line.split("\t");
                    return new Tuple2<String, String>(arr[4], arr[0]);

            }
        });
        //3、去重
        //rdd=[ (山西,香菜),(山西,大白菜),(湖南,上海青),(湖南,小龙虾) ,.....]
        JavaRDD<Tuple2<String, String>> rdd3 = rdd2.distinct();

        //4、转换数据类型
        //rdd = [山西->1,山西->1,湖南->1,湖南->1,...]

        JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, String> t1) throws Exception {
                return new Tuple2<String, Integer>(t1._1, 1);
            }
        });
        //5、按照省份分组统计菜的种类数
        //rdd = [山西->10,湖南->23,广东->32,天津->5...]
        JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //6、按照次数排序取出前三省份
        //rdd = [10->山西,23->湖南,32->广东,5->天津,...]
        JavaPairRDD<Integer, String> rdd6 = rdd5.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<Integer, String>(v1._2, v1._1);
            }
        });
        //rdd = [55->北京,42->河北,32->广东,23->湖南,...]
        JavaPairRDD<Integer, String> rdd7 = rdd6.sortByKey(false);
        //rdd = [北京->55,河北->42,广东->32,湖南->23,...]
        JavaPairRDD<String, Integer> rdd8 = rdd7.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2, t._1);
            }
        });

        List<Tuple2<String, Integer>> result = rdd8.take(3);

        //7、结果展示
        System.out.println(result);


    }
}
