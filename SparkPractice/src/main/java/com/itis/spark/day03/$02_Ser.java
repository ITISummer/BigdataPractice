package com.itis.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class $02_Ser {

    /**
     * 原因: spark算子中的call方法里面的逻辑是在task中执行的,算子call方法外面的代码是在Driver执行的,如果task中用到了driver的对象,spark会将该对象序列化之后传递给task使用,所以要求该对象必须要能够序列化
     * spark的序列化方式有两种:
     *      1、java序列化: 默认
     *      2、Kryo序列化
     *      java序列化的时候会将类的信息、类的继承信息、属性信息、类型信息、值的信息等都会序列化,序列化之后数据会相对比较大
     *      Kryo序列化的时候之后序列化类中的信息[属性、属性类型、值],序列化之后数据会相对比较小,性能和java序列化相比,块10倍左右
     *      在工作中建议使用Kryo序列化,性能更高
     * spark序列化的设置方式:
     *      1、在sparkconf中配置一个参数: new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
     *      2、设置哪些类使用指定序列化[可选]: registerKryoClasses（{类名.class,....}）
     */
    public static void main(String[] args) {

        Class[] arr = {Person.class};
        SparkConf conf = new SparkConf().setMaster("local[4]")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(arr)
                .setAppName("com.atguigu.spark.day01.RDDCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 5, 2, 8, 9, 10, 2));

        //final Person person = new Person();

        //报错: Person不能序列化
        //原因: person定义在driver中,在task使用,需要序列化之后传递给task使用
        //JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {
//
        //    public Integer call(Integer v1) throws Exception {
        //        return v1 * person.x;
        //    }
        //});

        //报错: Person不能序列化， 方法里面使用了对象自己的x属性的
        //JavaRDD<Integer> rdd2 = person.map(rdd1);


        //不报错: person对象在task创建,在task使用,不需要网络传递,不需要序列化
        JavaRDD<Integer> rdd2 = rdd1.map(new Function<Integer, Integer>() {

            public Integer call(Integer v1) throws Exception {
                return v1 * new Person().x;
            }
        });

        System.out.println(rdd2.collect());
    }
}
