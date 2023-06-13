import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.Calendar
import scala.collection.mutable.ListBuffer

/*
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

object ScalaSolution {
  def main(args: Array[String]): Unit = {
    scalaRDD()
    //    scalaRDDTest()
    //    scalaSql()
    //    scalaDSL()
  }

  /**
   * 使用 Scala RDD 算子做运算
   * [RDD Programming Guide](https://spark.apache.org/docs/3.3.1/rdd-programming-guide.html)
   * [transformations](https://spark.apache.org/docs/3.3.1/rdd-programming-guide.html#transformations)
   * [Spark算子总结](https://zhuanlan.zhihu.com/p/95746381)
   */
  private def scalaRDD(): Unit = {
    val conf = new SparkConf().setAppName("ScalaSolution").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("./data/good_promotion.csv")
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    // partition by brand order by stt
    val rdd1 = rdd.groupBy(line => line.split(",")(0))
      .mapValues(_.toList.sortBy(_.split(",")(1)))
    // 可以用 map() 替代 mapValues()
    //    .map { case (key, values) =>
    //    (key, values.toBuffer.sortBy((_:String).split(",")(1)))
    // 获取按brand分组后按stt排序后的列表，对每一个列表进行当前行前n行的 edt 最大值
    // max(edt) over (partition by brand order by stt rows between unbounded preceding and 1 preceding) maxEndDate
    val rdd2 = rdd1.mapValues(list => {
      // 将 List 转为 ListBuffer
      val listBuffer: ListBuffer[String] = ListBuffer.empty[String]
      listBuffer ++= list
      val size = listBuffer.size
      val maxEndDate = listBuffer.head.split(",")(2)
      listBuffer(0) = list.head + ",NULL"
      for (i <- 1 until size) {
        val pre = listBuffer(i - 1).split(",")
        if ("NULL".equals(pre(3))) {
          listBuffer(i) = list(i) + "," + pre(2)
        } else if (pre(3).compareTo(pre(2)) >= 0) {
          listBuffer(i) = list(i) + "," + pre(3)
        } else {
          listBuffer(i) = list(i) + "," + pre(2)
        }
      }

      // 判断 maxEndDate,stt,edt 大小
      // select brand, stt, edt, if(maxEndDate is null, stt, if(stt > maxEndDate, stt, date_add(maxEndDate, 1))) stt_new
      val tmpListBuffer: ListBuffer[String] = ListBuffer.empty[String]
      listBuffer.foreach(ele => {
        val splits = ele.split(",")
        val dd = Calendar.getInstance()
        if (!"NULL".equals(splits(3))) {
          dd.setTime(sdf.parse(splits(3)))
          dd.add(Calendar.DAY_OF_MONTH, 1);
        }
        // 给新 tmpListBuffer 填充内容
        tmpListBuffer += (ele + "," + (if ("NULL".equals(splits(3))) splits(1) else if (splits(1).compareTo(splits(3)) > 0) splits(1) else sdf.format(dd.getTime)))
      })

      val tmp2ListBuffer = ListBuffer[String]()
      //       datediff(edt,stt_new) -> datediff(splits[2],splits[4])
      tmpListBuffer.foreach(ele => {
        val splits = ele.split(",")
        //        val res: ListBuffer[String] = ListBuffer.empty[String]
        val date1 = LocalDate.parse(splits(2)) //edt
        val date2 = LocalDate.parse(splits(4)) // stt_new
        tmp2ListBuffer += (splits(0) + "," + ChronoUnit.DAYS.between(date2, date1))
      })

      // select brand, sum(if(day_nums >=0, day_nums + 1,0)) day_nums
      var sumDays = 0
      val resListBuffer = ListBuffer[Int]()
      tmp2ListBuffer.foreach(ele => {
        val splits = ele.split(",")
        if (splits(1).toInt >= 0) {
          sumDays += (splits(1).toInt + 1)
        }
      })
      resListBuffer += sumDays
      resListBuffer.toList
    })

    val res = rdd2
    println(res.collect().mkString("Array(", ", ", ")"))
    sc.stop()
  }

  /**
   * 测试 Scala 的 RDD 算子
   */
  private def scalaRDDTest(): Unit = {
    val conf = new SparkConf().setAppName("MySparkApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 创建一个 RDD
    val data = List(1, 2, 3, 4, 5)
    val rdd = sc.parallelize(data)

    // 使用 RDD 算子进行转换和操作
    val result = rdd.filter(_ > 2).map(_ * 2).collect()

    // 打印结果
    result.foreach(println)

    // 关闭 SparkContext
    sc.stop()
  }


  /**
   * 使用 Scala Sql 实现
   * 询问自 ChatGPT
   */
  private def scalaSql(): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSolution")
      .master("local[*]")
      .getOrCreate()

    // 读取 good_promotion 表并创建临时视图
    val goodPromotionDF = spark.read
      .format("csv")
      .option("header", true)
      .load("./data/good_promotion_sql.csv");
    goodPromotionDF.createOrReplaceTempView("good_promotion")

    // 使用 Spark SQL 执行查询
    val resultDF = spark.sql(
      """
        |SELECT brand, sum(if(day_nums >= 0, day_nums + 1, 0)) as day_nums
        |FROM (
        |  SELECT brand, datediff(edt, stt_new) as day_nums
        |  FROM (
        |    SELECT brand, stt, if(maxEndDate is null, stt, if(stt > maxEndDate, stt, date_add(maxEndDate, 1))) as stt_new, edt
        |    FROM (
        |      SELECT brand, stt, edt,
        |             max(edt) over (partition by brand order by stt rows between unbounded preceding and 1 preceding) as maxEndDate
        |      FROM good_promotion
        |    ) t1
        |  ) t2
        |) t3
        |GROUP BY brand
        |""".stripMargin)

    // 显示结果
    resultDF.show()

    // 关闭 SparkSession
    spark.close()
  }

  /**
   * 使用 Scala sql DSL 语法
   * 询问自 ChatGPT
   */
  private def scalaDSL(): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSolution")
      .master("local[*]")
      .getOrCreate()

    // 读取 good_promotion 表并创建 DataFrame
    val goodPromotionDF = spark.read.format("csv").option("header", true).load("./data/good_promotion_sql.csv");
    //    goodPromotionDF.createOrReplaceTempView("good_promotion")

    // 定义子查询 t1
    val t1 = goodPromotionDF.withColumn("maxEndDate", max("edt").over(Window.partitionBy("brand").orderBy("stt").rowsBetween(Window.unboundedPreceding, -1)))

    // 定义子查询 t2
    val t2 = t1.withColumn("stt_new", when(col("maxEndDate").isNull, col("stt")).otherwise(when(col("stt") > col("maxEndDate"), col("stt")).otherwise(date_add(col("maxEndDate"), 1))))

    // 定义子查询 t3
    val t3 = t2.withColumn("day_nums", datediff(col("edt"), col("stt_new")))
      .select("brand", "day_nums")

    // 执行查询并分组求和
    val resultDF = t3.groupBy("brand")
      .agg(sum(when(col("day_nums") >= 0, col("day_nums") + 1).otherwise(0)).as("day_nums"))

    // 显示结果
    resultDF.show()

    // 关闭 SparkSession
    spark.close()
  }
}