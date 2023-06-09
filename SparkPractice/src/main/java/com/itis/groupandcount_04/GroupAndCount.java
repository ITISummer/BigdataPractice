package com.itis.groupandcount_04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class GroupAndCount implements Serializable{

    @Test
    public void run() {

        SparkConf conf = new SparkConf().setAppName("SparkSolution").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("input/score.csv")
                .mapToPair(line -> {
                    String[] splits = line.split(",");
                    return new Tuple2<>(splits[1], new Score(splits[2], Integer.valueOf(splits[3])));

                }).groupByKey()
                .mapValues(scores -> {
                    // 将学科名和学科成绩封装为一个 TreeMap
                    Map<String, Integer> map = new HashMap<>();
                    for (Score score : scores) {
                        Integer v = map.get(score.getCourse());
                        // key 重复的保留 value 值大的
                        if (v != null && v.compareTo(score.getGrade()) > 0) {
                            continue;
                        }
                        map.put(score.getCourse(), score.getGrade());
                    }
                    // Map 按 value 排序
                    List<Map.Entry<String,Integer>> list = sortMap(map);
                    int len = list.size();
                    StringBuilder top2Scores = new StringBuilder();
                    if(len == 1) {
                        top2Scores.append(list.get(0).getValue());
                    } else {
                        top2Scores.append(list.get(0).getValue()).append(",").append(list.get(1).getValue());
                    }
                    return len+","+top2Scores.toString();
                })
                .map(v1 -> v1._1 + "," + v1._2)
                .saveAsTextFile("output/practice");
//                .collect()
//                .forEach(System.out::println);
        sc.close();
    }

    /**
     * Map 按 value 倒序排序
     * @return 返回按值排序后的 Map
     */
    public List<Map.Entry<String,Integer>> sortMap(Map<String,Integer> map) {
         // 构建包含所有键值对的列表，并按值进行排序
        List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(map.entrySet());
        sortedList.sort(new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                return -entry1.getValue().compareTo(entry2.getValue());
            }
        });

        return sortedList;
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
class Score implements Serializable {
    private String course;
    private Integer grade;
}
