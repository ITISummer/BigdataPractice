package com.itis.spark.day05;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
public class Test {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        //读取数据
        spark.read().option("sep","\t")
                .csv("datas/sql/user_visit_action.txt")
                .toDF("dt","user_id","session_id","page_id","action_time","search_keyword",
                        "click_category_id","click_product_id","order_category_ids",
                        "order_product_ids","pay_category_ids","pay_product_ids","city_id")
                .selectExpr("click_product_id","city_id")
                .createOrReplaceTempView("user_visit_action");

        spark.read().option("sep","\t").csv("datas/sql/city_info.txt")
                .toDF("city_id","city_name","area")
                .createOrReplaceTempView("city_info");

        spark.read().option("sep","\t").csv("datas/sql/product_info.txt")
                .toDF("product_id","product_name","extend_info")
                .selectExpr("product_id","product_name")
                .createOrReplaceTempView("product_info");

        CityAgg cityAgg = new CityAgg();
        UserDefinedFunction udaf = functions.udaf(cityAgg, Encoders.STRING());
        spark.udf().register("city_mark",udaf);

        //获取所有点击行为数据,并关联城市表和产品表得到点击产品名称和城市、区域
        spark.sql("select\n" +
                "\ta.click_product_id,c.product_name,a.city_id,b.city_name,b.area\n" +
                "from user_visit_action a left join city_info b\n" +
                "on a.city_id = b.city_id\n" +
                "left join product_info c\n" +
                "on a.click_product_id = c.product_id\n" +
                "where a.click_product_id !='-1'").createOrReplaceTempView("click_product");

        //统计每个区域每个产品的点击次数
        spark.sql("select\n" +
                "\tarea,product_name,\n" +
                "\tcount(1) click_num,\n" +
                "\tcity_mark(city_name) mark\n" +
                "from click_product\n" +
                "group by area,product_name").createOrReplaceTempView("area_product_click");

        spark.sql("select\n" +
                "\tarea,product_name,click_num,mark\n" +
                "from(\n" +
                "select\n" +
                "\tarea,product_name,click_num,mark,\n" +
                "\trow_number() over(partition by area order by click_num desc) rn\n" +
                "from area_product_click\n" +
                ") t1 where rn<=3").write().mode(SaveMode.Overwrite).csv("output");
    }
}
