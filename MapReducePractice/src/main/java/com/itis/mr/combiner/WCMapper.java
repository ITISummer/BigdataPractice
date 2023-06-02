package com.itis.mr.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    在该类中实现需要在MapTask中需要实现的功能

    自定义一个类并继承Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        KEYIN ：读取的内容的偏移量的类型
        VALUEIN ：读取的内容（一行一行的内容）的类型

        KEYOUT : 写出的key的类型（在这是单词的类型）
        VALUEOUT : 写出的value的类型（在这是单词数量的类型）
 */
public class WCMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    private Text outKey = new Text();//创建key的对象
    private LongWritable outValue = new LongWritable();//创建value的对象

    /**
     * 在该方法中实现需要在MapTask中实现的功能
     * 注意：该方法在被循环调用每调用一次传入一行数据
     * @param key 读取的内容的偏移量
     * @param value 读取的内容（一行一行的内容）
     * @param context 上下文（在这用来将key,value写出去）
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //1.将读进来的一行数据进行切割
        //1.1将Text转成String
        String line = value.toString(); // aaa ccc
        //1.2将数据进行切割
        String[] words = line.split(" ");//{"aaa","ccc"}

        //2.封装key,value
        //2.1遍历数组
        for (String word : words) { //(单词,1)
            //2.2封装key,value
            outKey.set(word);//给key赋值
            outValue.set(1);//给value赋值

            //3.将key,value写出去
            context.write(outKey,outValue);
        }
    }
}












