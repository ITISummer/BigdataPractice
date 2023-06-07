package com.itis.spark.day02;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class $02_Transformation implements Serializable {

    /**
     * spark算子分为两种:
     *      Transformation[转换]算子: 会生成新的RDD,不会触发任务的计算
     *      Action[行动]算子: 不会生成新的RDD,一般要么没有结果要么结果是Java的类型,触发任务的计算
     */

    /**
     *  map: 映射[转换]
     *      map是针对每个元素处理,处理完成之后返回一个结果,结果可以是任意类型
     *      map生成的新RDD的元素个数 = 原RDD的元素个数
     *      map的使用场景:一般用于一对一转换
     *      map类比SQL的UDF函数
     */
    @Test
    public void map(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 5, 8, 2, 9, 3, 5));

        JavaRDD<String> rdd2 = rdd1.map(new Function<Integer, String>() {
            public String call(Integer x) throws Exception {
                return x * 10 +"";
            }
        });

        List<String> result = rdd2.collect();

        for(String x: result){
            System.out.println(x);
        }
    }

    /**
     * flatMap: 转换 + 炸裂[explode]
     *      flatMap是针对每个元素处理,处理完成之后要求返回一个集合类型的结果
     *      flatMap后续会自动将集合干掉,只保留元素
     *      flatMap的使用场景: 用于一对多
     *
     */
    @Test
    public void flatMap(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("hello java", "spark hadoop", "spark hadoop", "flume kafka hive"));

        //第一个泛型是代表参数类型,第一个泛型是代表返回的集合中的元素类型
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {

                String[] arr = s.split(" ");

                List<String> list = Arrays.asList(arr);

                return list.iterator();
            }
        });

        List<String> result = rdd2.collect();

        for(String x: result){
            System.out.println(x);
        }
    }

    /**
     * groupBy: 按照指定的字段进行分组
     *      groupBy针对的是每个元素,返回的是分组的字段
     *      groupBy返回的新RDD的元素类型是KV键值对,K就是分组的字段的值,V就是K对应的原RDD所有元素的集合
     *      groupBy类比SQL的gorupBy
     *      groupBy会产生shuffle
     */
    @Test
    public void groupBy(){


        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("1,wangwu,beijing", "2,zhaoliu,shanghai", "3,hanmeimei,beijing", "4,lilei,beijing"));

        JavaPairRDD<String, Iterable<String>> rdd2 = rdd1.groupBy(new Function<String, String>() {
            public String call(String v1) throws Exception {
                return v1.split(",")[2];
            }
        });

        List<Tuple2<String, Iterable<String>>> result = rdd2.collect();

        for( Tuple2<String, Iterable<String>> t : result ){

            System.out.println( t._1 + "--->" + t._2);
        }

    }

    /**
     * filter: 过滤
     *      filter针对每个元素处理, filter里面的方法会返回一个boolean,如果返回为true代表保留
     */
    @Test
    public void filter(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 6, 3, 8, 0, 2));

        JavaRDD<Integer> rdd2 = rdd1.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        System.out.println(rdd2.collect());
    }

    /**
     * distinct: 去重
     * distinct会产生shuffle操作
     *
     * MR过程: 数据 -> InputFormat[切片、kv] -> map方法 -> 环形缓冲区[80%, 分区排序] -> combiner ->磁盘[文件] -> 合并小文件 -> reducer拉取数据 -> 归并排序 -> reducer方法 -> outputFormat  ->磁盘
     * MR shuffle:   -> 环形缓冲区[80%, 分区排序] -> combiner ->磁盘[文件] -> 合并小文件 -> reducer拉取数据 -> 归并排序
     * spark shuffle:   -> 缓冲区 [分区[排序]] -> combiner ->磁盘[文件] -> 合并小文件 -> 子RDD分区拉取数据 -> 归并[排序]
     */
    @Test
    public void distinct(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 4, 2, 6, 8, 2, 8, 0, 1, 2));

        JavaRDD<Integer> rdd2 = rdd1.distinct();

        System.out.println(rdd2.collect());
    }

    /**
     * sortBy: 根据指定字段排序
     *      sortBy是针对每个元素处理,后续根据返回值对元素进行排序
     *      sortBy会产生shuffle
     */
    @Test
    public void sortBy(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,4,7,2,8,0,2,7,3));

        JavaRDD<Integer> rdd3 = rdd1.sortBy(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, false, 4);

        System.out.println(rdd3.collect());
    }

    /**
     *  mapValues: 针对value值做转换,key保持不变
     *          mapValues是针对value值操作,操作完成之后需要返回一个新的value值
     */
    @Test
    public void mapValues(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("aa,1", "bb,2", "cc,3", "dd,4"));

        //第一个泛型是代表参数类型
        //第二个泛型是代表返回的key的类型
        //第三个泛型是代表返回的value的类型
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String key = s.split(",")[0];
                Integer value = Integer.parseInt(s.split(",")[1]);
                return new Tuple2<String, Integer>(key, value);
            }
        });

        JavaPairRDD<String, Integer> rdd3 = rdd2.mapValues(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 10;
            }
        });

        //使用map实现
        JavaPairRDD<String, Integer> rdd4 = rdd2.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<String, Integer>(v1._1, v1._2 * 10);
            }
        });

        System.out.println(rdd4.collect());

    }


    /**
     * groupByKey: 根据key分组
     *      groupByKey生成新RDD的元素是KV键值对,K是原RDD分组的key,V是Key对应原RDD所有的value值的集合
     */
    @Test
    public void groupByKey(){
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("aa,1", "bb,2", "cc,3", "aa,4","cc,10","dd,6"));

        //第一个泛型是代表参数类型
        //第二个泛型是代表返回的key的类型
        //第三个泛型是代表返回的value的类型
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String key = s.split(",")[0];
                Integer value = Integer.parseInt(s.split(",")[1]);
                return new Tuple2<String, Integer>(key, value);
            }
        });

        JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd2.groupByKey();

        //使用gorupBy实现groupByKey
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> rdd4 = rdd2.groupBy(new Function<Tuple2<String, Integer>, String>() {
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1;
            }
        });

        JavaPairRDD<String, Iterable<Integer>> rdd5 = rdd4.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Iterable<Integer>>() {
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> v1) throws Exception {
                //v1 = ( aa, [(aa,1), (aa,4)] )
                ArrayList<Integer> values = new ArrayList<Integer>();
                Iterator<Tuple2<String, Integer>> it = v1._2.iterator();
                while (it.hasNext()) {
                    Tuple2<String, Integer> t1 = it.next();
                    //t1 = (aa,1)
                    values.add(t1._2);
                }

                return new Tuple2<String, Iterable<Integer>>(v1._1, values);
            }
        });

        System.out.println(rdd5.collect());

        System.out.println(rdd3.collect());
    }

    /**
     * reduceByKey: 先按照key分组,对分组之后的每个key的所有value值聚合
     *
     * groupByKey和reduceByKey的区别:
     *      groupByKey: 只是单纯按照key分组, groupByKey没有预聚合
     *      reduceByKey: 按照key分组 + 聚合,reduceByKey有预聚合功能[combiner],从性能上来说,reduceByKey的性能比groupByKey要高,所以工作中推荐使用这种高性能的shuffle算子
     */
    @Test
    public void reduceByKey(){
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("aa,1", "bb,2", "cc,3", "aa,4","cc,10","dd,6"));

        //第一个泛型是代表参数类型
        //第二个泛型是代表返回的key的类型
        //第三个泛型是代表返回的value的类型
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String key = s.split(",")[0];
                Integer value = Integer.parseInt(s.split(",")[1]);
                return new Tuple2<String, Integer>(key, value);
            }
        });

        //第一个泛型是代表聚合的临时变量类型[每组第一次计算的时候,临时变量的初始值 = 第一个value值]
        //第二个泛型是代表待聚合的value值类型
        //第三个泛型是代表本次聚合的结果类型
        JavaPairRDD<String, Integer> rdd3 = rdd2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer sum, Integer x) throws Exception {
                System.out.println("sum="+sum+",x="+x);
                return sum + x;
            }
        });

        System.out.println(rdd3.collect());
    }

    /**
     * sortByKey: 按照key对元素排序
     */
    @Test
    public void sortByKey(){

        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("aa,1", "bb,2", "cc,3", "aa,4","cc,10","dd,6"));

        //第一个泛型是代表参数类型
        //第二个泛型是代表返回的key的类型
        //第三个泛型是代表返回的value的类型
        // rdd2 = [ aa->1, bb->2,cc->3,aa->4,cc->10,dd->6]
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String key = s.split(",")[0];
                Integer value = Integer.parseInt(s.split(",")[1]);
                return new Tuple2<String, Integer>(key, value);
            }
        });

        // rdd3 = [ 1 -> aa, 2 -> bb,3 -> cc,4->aa,10->cc,6->dd]
        JavaPairRDD<Integer, String> rdd3 = rdd2.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<Integer, String>(v1._2, v1._1);
            }
        });

        JavaPairRDD<Integer, String> rdd4 = rdd3.sortByKey(false);

        System.out.println(rdd4.collect());


    }

    @Test
    public void test(){
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //rdd2 = ["1,lisi,打篮球-爬山-游泳", "2,wangwu,看书-打篮球"]
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("1,lisi,打篮球-爬山-游泳", "2,wangwu,看书-打篮球"));

        //rdd3 = [ "1,lisi,打篮球", "1,lisi,爬山" ,"1,lisi,游泳" , "2,wangwu,看书","2,wangwu,打篮球" ]
        JavaRDD<String> rdd3 = rdd2.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] arr = s.split(",");
                String id = arr[0];
                String name = arr[1];
                String[] xqarr = arr[2].split("-");

                ArrayList<String> result = new ArrayList<String>();

                for (String xq : xqarr) {
                    result.add(id + "," + name + "," + xq);
                }
                return result.iterator();
            }
        });
        // 分组+聚合 = group by  + map
        //rdd4=[ 打篮球->List( "1,lisi,打篮球" ,"2,wangwu,打篮球")  ,  爬山-> List( "1,lisi,爬山" ) ,游泳 -> List( "1,lisi,游泳" ), 看书-> List( "2,wangwu,看书" ) ]

        //JavaPairRDD<String, Iterable<String>> rdd4 = rdd3.groupBy(new Function<String, String>() {
        //    public String call(String v1) throws Exception {
        //        return v1.split(",")[2];
        //    }
        //});
//
        ////按照兴趣统计人数
        ////rdd5 = [ 打篮球->2  ,  爬山-> 1,游泳 -> 1, 看书->1 ]
        //JavaRDD<Tuple2<String, Integer>> rdd5 = rdd4.map(new Function<Tuple2<String, Iterable<String>>, Tuple2<String, Integer>>() {
        //    public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> v1) throws Exception {
        //        return new Tuple2<String, Integer>(v1._1, IterableUtils.toList(v1._2).size());
        //    }
        //});

        // 分组+聚合 = reduceByKey
        //rdd4 = [ 打篮球 -> 1, 爬山 -> 1 ,游泳 -> 1 , 看书 -> 1,打篮球->1 ]
        JavaPairRDD<String, Integer> rdd4 = rdd3.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split(",")[2], 1);
            }
        });

        JavaPairRDD<String, Integer> rdd5 = rdd4.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //按照人数降序排列
        //JavaRDD<Tuple2<String, Integer>> rdd6 = rdd5.sortBy(new Function<Tuple2<String, Integer>, Integer>() {
        //    public Integer call(Tuple2<String, Integer> v1) throws Exception {
        //        return v1._2;
        //    }
        //}, true, 4);

        //List<Tuple2<String, Integer>> result = rdd6.collect();
        List<Tuple2<String, Integer>> result = rdd5.collect();

        for(Tuple2 t : result){
            System.out.println(t);
        }


    }
}
