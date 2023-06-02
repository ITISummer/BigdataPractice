package com.itis.mr.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    只能对Key进行排序--所以FlowBean作为key
 */
public class CPMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    //创建的key的对象
    private  FlowBean outKey = new FlowBean();
    //创建的value的对象
    private  Text outValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        //1.切割数据
        String[] flowInfo = value.toString().split("\t");
        //2.封装key,value
        //给key赋值
        outKey.setUpFlow(Long.parseLong(flowInfo[1]));
        outKey.setDownFlow(Long.parseLong(flowInfo[2]));
        outKey.setSumFlow(Long.parseLong(flowInfo[3]));
        //给value赋值
        outValue.set(flowInfo[0]);
        //3.将key,value写出去
        context.write(outKey,outValue);
    }
}
