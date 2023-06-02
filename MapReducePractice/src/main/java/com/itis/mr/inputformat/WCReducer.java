package com.itis.mr.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    在该类中实现需要在ReduceTask中需要实现的功能

    自定一个类并继承 Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        KEYIN: 读取的key的类型（Mapper写出的key的类型）
        VALUEIN：读取的value的类型（Mapper写出的value的类型）

        KEYOUT ： 写出的key的类型（在这是单词的类型）
        VALUEOUT：写出的value的类型（在这是单词数量的类型）
 */
public class WCReducer  extends Reducer<Text, LongWritable,Text,LongWritable> {
    private LongWritable outValue = new LongWritable();//创建的value对象

    /**
     * 在该方法中实现需要在ReduceTask中需要实现的功能
     * 注意：该方法在被循环调用每调用一次传入一组数据（在这key值相同为一组）
     *
     * @param key 读取的key
     * @param values 读取的所有的value
     * @param context 上下文（在这用来将key,value写出去）
     * @throws IOException
     * @throws InterruptedException
     *
     * Text  LongWritable
     * aaa      1           =======>  (aaa,2)
     * aaa      1
     * ===============
     * ccc      1
     * ccc      1
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long sum = 0;//累加数量
        //1.将value累加
        //1.1遍历values
        for (LongWritable value : values) {
            //将LongWritable转成long类型
            long v = value.get();
            //累加
            sum += v;
        }

        //2.封装key,value
        outValue.set(sum);

        //3.将key,value写出去
        context.write(key,outValue);

    }
}








