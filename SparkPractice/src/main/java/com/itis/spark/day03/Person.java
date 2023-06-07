package com.itis.spark.day03;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class Person {

    public int x = 10;


    public JavaRDD<Integer> map(JavaRDD<Integer> rdd) {
        JavaRDD<Integer> rdd2 = rdd.map(new Function<Integer, Integer>() {

            public Integer call(Integer v1) throws Exception {

                return v1 * x;

            }
        });
        return rdd2;
    }


}
