package com.itis.spark;

import org.apache.commons.collections4.IterableUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/*
-- spark rdd 算子 解法
-- 解法一：参考：[HiveSQL——打折日期交叉问题](https://www.cnblogs.com/wdh01/p/16898604.html)
select brand, sum(if(day_nums >= 0, day_nums + 1, 0)) day_nums
from (select brand, datediff(edt, stt_new) day_nums
      from (select brand, stt, if(maxEndDate is null, stt, if(stt > maxEndDate, stt, date_add(maxEndDate, 1))) stt_new, edt
            from (select brand, stt, edt,
                         max(edt) over (partition by brand order by stt rows between unbounded preceding and 1 preceding) maxEndDate
                  from good_promotion
                 )t1
           )t2
     )t3
group by brand;
 */

/**
 * @author 吕长旭
 */
public class SparkSolution implements Serializable {
    @Test
    public void sparkRDD() {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkRDD");
        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 3. 编写代码--读取路径./data下的文件，并创建RDD
        JavaRDD<String> rdd1 = sc.textFile("./data/good_promotion.csv");

        // 先进行分组 partition by brand
        JavaPairRDD<String, Iterable<String>> rdd2 = rdd1.groupBy(new Function<String, String>() {
            public String call(String v1) throws Exception {
                return v1.split(",")[0];
            }
        });

        // 对分组后的 key-value 的 value 里的元素进行切割后按下标[1]内部排序
        JavaRDD<Tuple2<String, Iterable<String>>> rdd3 = rdd2.map(new Function<Tuple2<String, Iterable<String>>, Tuple2<String, Iterable<String>>>() {
            @Override
            public Tuple2<String, Iterable<String>> call(Tuple2<String, Iterable<String>> t) throws Exception {
                // [Converting Iterable to Collection in Java](https://www.baeldung.com/java-iterable-to-collection)
                List<String> list = IterableUtils.toList(t._2);
                // 普通匿名对象比较方式
//                list.sort(new Comparator<String>() {
//                    @Override
//                    public int compare(String o1, String o2) {
//                        return o1.split(",")[1].compareTo(o2.split(",")[1]);
//                    }
//                });
                // lambda
                list.sort((o1, o2) -> o1.split(",")[1].compareTo(o2.split(",")[1]));
                // 求当前行n的 [0,n) 行的 edt 的最大值然后拼接到当前行的末尾，如果没有则拼接 NULL
                // order by stt rows between unbounded preceding and 1 preceding
                int size = list.size();
                String maxEndDate = list.get(0).split(",")[2];
                // 第一个元素最后必然是 ",NULL"
                list.set(0, list.get(0) + ",NULL");
                for (int i = 1; i < size; i++) {
                    String[] splits = list.get(i - 1).split(",");
                    if ("NULL".equals(splits[3])) {
                        list.set(i, list.get(i) + "," + splits[2]);
                    } else if (splits[3].compareTo(splits[2]) >= 0) {
                        list.set(i, list.get(i) + "," + splits[3]);
                    } else {
                        list.set(i, list.get(i) + "," + splits[2]);
                    }
                }

                // 判断 maxEndDate,stt,edt 大小
                // select brand, stt, edt, if(maxEndDate is null, stt, if(stt > maxEndDate, stt, date_add(maxEndDate, 1))) stt_new
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                List<String> modifiedList = new ArrayList<>();
                list.forEach(ele -> {
                    String[] splits = ele.split(",");
                    try {
                        Calendar dd = Calendar.getInstance();
                        if (!"NULL".equals(splits[3])) {
                            dd.setTime(sdf.parse(splits[3]));
                            dd.add(Calendar.DAY_OF_MONTH, 1);
                        }
                        ele += "NULL".equals(splits[3]) ? "," + splits[1] : splits[1].compareTo(splits[3]) > 0 ? "," + splits[1] : "," + sdf.format(dd.getTime());
                        modifiedList.add(ele);
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                });

                // datediff(edt, stt_new) -> datediff(splits[2],splits[4])
                List<String> newList = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    String[] splits = modifiedList.get(i).split(",");
                    StringBuilder sb = new StringBuilder();
                    LocalDate date1 = LocalDate.parse(splits[2]); // edt
                    LocalDate date2 = LocalDate.parse(splits[4]);  // stt_new
                    sb.append(splits[0]).append(",").append(ChronoUnit.DAYS.between(date2, date1));
                    newList.add(sb.toString());
                }

                // select brand, sum(if(day_nums >= 0, day_nums + 1, 0)) day_nums
                List<String> newList2 = new ArrayList<>(size);
                int sumDays = 0;
                String[] splits = null;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < size; i++) {
                    splits = newList.get(i).split(",");
                    if (Integer.parseInt(splits[1]) >= 0) {
                        sumDays += Integer.parseInt(splits[1]) + 1;
                    }
                }
                sb.append(splits[0] + ",").append(sumDays);
                newList2.add(sb.toString());

                list = newList2;
                return new Tuple2<>(t._1, list);
            }
        });
        // action 算子 collect() 进行收集后输出
        List<Tuple2<String, Iterable<String>>> result = rdd3.collect();
        for (Tuple2<String, Iterable<String>> t : result) {
            System.out.println(t._1 + "-->" + t._2);
        }
        // 4. 关闭sc
        sc.stop();
    }

    /**
     * spark sql
     */
    @Test
    public void sparkSQL() {
        // 1. 创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL");

        // 2. 创建SparkContext
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 读取文件内容,按照json line解析
        Dataset<Row> lineDS = spark.read()
                .option("header", "true") // 设置区分表头
                .csv("./data/good_promotion_sql.csv");

        // 4. 根据lineDS创建临时视图,视图的生命周期和sparkSession绑定，“orReplace”表示可覆盖
        lineDS.createOrReplaceTempView("good_promotion");
        Dataset<Row> resultDS = spark.sql(
                " select brand, sum(if(day_nums >= 0, day_nums + 1, 0)) day_nums " +
                        "from (select brand, datediff(edt, stt_new) day_nums " +
                        "from (select brand, stt, if(maxEndDate is null, stt, if(stt > maxEndDate, stt, date_add(maxEndDate, 1))) stt_new, edt " +
                        "from (select brand, stt, edt, max(edt) over (partition by brand order by stt rows between unbounded preceding and 1 preceding) maxEndDate from good_promotion " +
                        ")t1 " +
                        ")t2 " +
                        ")t3 " +
                        "group by brand; ");
        // 5. 输出结果
        resultDS.show();
        // 6. 关闭spark
        spark.close();
    }


    /**
     * spark sql DSL
     */
    @Test
    public void sparkDSL() {
        // 1. 创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_DSL");

        // 2. 创建SparkContext
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 读取文件内容,按照jsonline解析
        Dataset<Row> lineDS = spark.read()
                .option("header", "true") // 设置区分表头
                .csv("./data/good_promotion_sql.csv");

        // 4. 根据lineDS创建临时视图,视图的生命周期和sparkSession绑定，"orReplace"表示可覆盖
        Dataset<Row> resultDS01 = lineDS.selectExpr("brand", "stt", "edt", "max(edt) over(partition by brand order by stt rows between unbounded preceding and 1 preceding) as maxEndDate")
                .selectExpr("brand", "stt", "if(maxEndDate is null,stt,if(stt>maxEndDate, stt,date_add(maxEndDate,1))) stt_new", "edt")
                .selectExpr("brand", "datediff(edt,stt_new) day_nums")
                .groupBy("brand")
                .agg(sum(when(col("day_nums").$greater$eq(0), col("day_nums").plus(1)).otherwise(0)).alias("day_nums"));
        resultDS01.show();
        // 6. 关闭spark
        spark.close();
    }
}
