package com.itis.top10_03;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 在此编写你的逻辑，输入的文件在input文件夹
 * 将结果返回到output/practice文件夹
 */
public class Top10 {
    @Test
    public void run() {
        //////////在这里实现代码逻辑///////////
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("SparkSolution");
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        JavaRDD<String> rdd = sc.textFile("input/user_visit_action.txt");

        List<CategoryCountInfo> result = rdd.map(line -> { // 将 category ids 切割出来
                    String[] splits = line.split("_");
                    return new Tuple3<>(splits[6], splits[8], splits[10]);
                }).flatMapToPair(action -> {
                    ArrayList<Tuple2<String, CategoryCountInfo>> res = new ArrayList<>();
                    if (!"-1".equals(action._1())) { // 点击日志
                        String[] visitIds = action._1().split(",");
                        for (String id : visitIds) {
                            res.add(new Tuple2<>(id, new CategoryCountInfo(id, 1, 0, 0)));
                        }
                    } else if (!"null".equals(action._2())) { // 下单日志
                        String[] orderIds = action._2().split(",");
                        for (String id : orderIds) {
                            res.add(new Tuple2<>(id, new CategoryCountInfo(id, 0, 1, 0)));
                        }
                    } else if (!"null".equals(action._3())) { // 支付日志
                        String[] payIds = action._3().split(",");
                        for (String id : payIds) {
                            res.add(new Tuple2<>(id, new CategoryCountInfo(id, 0, 0, 1)));
                        }
                    }
                    return res.iterator(); // 这里要返回 iterator 才行
                }).reduceByKey((sum, ele) -> {
                    sum.setClickCount(sum.getClickCount() + ele.getClickCount());
                    sum.setOrderCount(sum.getOrderCount() + ele.getOrderCount());
                    sum.setPayCount(sum.getPayCount() + ele.getPayCount());
                    return sum;
                }).map(v1 -> v1._2)
//                .sortBy(ele -> new TupleComparator(), false, 2)
                .sortBy(v1 -> v1, false, 2)
                .take(10);

        // 收集结果
        sc.parallelize(result)
//                .map(v1 -> v1._1() + "," + v1._2() + "," + v1._3() + "," + v1._4())
                .map(v1 -> v1.getCategoryId() + "," + v1.getClickCount() + "," + v1.getOrderCount() + "," + v1.getPayCount())
                .collect().forEach(System.out::println);
//                .saveAsTextFile("output/practice");
        // 关闭资源
        sc.close();
        ///////////////////////////////////
    }

//    static class TupleComparator implements Comparator<Tuple4<String, Integer, Integer, Integer>> , Serializable {
//        @Override
//        public int compare(Tuple4<String, Integer, Integer, Integer> o1, Tuple4<String, Integer, Integer, Integer> o2) {
//            // 小于返回-1,等于返回0,大于返回1
//            if (o1._2().equals(o2._2())) {
//                if (o1._3().equals(o2._3())) {
//                    if (o1._4().equals(o2._4())) {
//                        return 0;
//                    } else {
//                        return o1._4() < o2._4() ? -1 : 1;
//                    }
//                } else {
//                    return o1._3() < o2._3() ? -1 : 1;
//                }
//            } else {
//                return o1._2() < o2._2() ? -1 : 1;
//            }
//        }
//    }
//

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    private static class CategoryCountInfo implements Serializable, Comparable<CategoryCountInfo> {
        private String categoryId;
        private Integer clickCount;
        private Integer orderCount;
        private Integer payCount;

        /*
         * 传入参数 ：下一个对象
         * 返回值 = 0 当前和传入相等
         * 返回值 > 0 当前大
         * 返回值 < 0 传入的大
         * */
        @Override
        public int compareTo(CategoryCountInfo o) {
            // 小于返回-1,等于返回0,大于返回1
            if (this.getClickCount().equals(o.getClickCount())) {
                if (this.getOrderCount().equals(o.getOrderCount())) {
                    if (this.getPayCount().equals(o.getPayCount())) {
                        return 0;
                    } else {
                        return this.getPayCount() < o.getPayCount() ? -1 : 1;
                    }
                } else {
                    return this.getOrderCount() < o.getOrderCount() ? -1 : 1;
                }
            } else {
                return this.getClickCount() < o.getClickCount() ? -1 : 1;
            }

        }
    }
}
