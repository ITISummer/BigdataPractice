package com.itis.spark.day05;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;
public class $01_UDF {

    /**
     * 自定义UDF函数
     *      1、静态导入udf方法: import static org.apache.spark.sql.functions.udf;
     *      2、通过udf方法定义函数
     *      3、注册udf方法到sparksession中: spark.udf.register("sql使用的函数名", 第二步定义的函数对象)
     *      4、sql中使用: spark.sql("select .. from ..")
     * @param args
     */
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate();

        Dataset<String> df = spark.read().textFile("datas/test.txt");

        //员工id正常是8位,现在要对不满8位的员工id，左侧以0补齐
        df.createOrReplaceTempView("test");

        //UDFN代表定义一个N个参数的UDF方法
        //前面N个泛型代表参数类型
        //最后一个泛型是代表返回值的类型
        UserDefinedFunction func = udf(new UDF3<String, Integer, String, String>() {
            public String call(String id, Integer len, String pad) throws Exception {
                //id = "1001"  len=8  pad='0'
                int idLength = id.length();
                int padLength = len - idLength;
                for (int i = 1; i <= padLength; i++) {
                    id = pad + id;
                }
                return id;
            }
        }, DataTypes.StringType);

        //注册udf函数
        spark.udf().register("myLpad",func);

        spark.sql("select myLpad(value,8,'0') from test").show();
    }
}
