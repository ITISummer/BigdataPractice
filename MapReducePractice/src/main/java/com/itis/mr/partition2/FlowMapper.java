package com.itis.mr.partition2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    在该类中实现需要在MapTask中需要实现的功能

    自定义一个类并继承Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        KEYIN ：读取的内容的偏移量的类型
        VALUEIN ：读取的内容（一行一行的内容）的类型

        KEYOUT : 写出的key的类型（在这是手机号类型）
        VALUEOUT : 写出的value的类型（在这是FlowBean类型）
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    //创建key的对象
    private Text outKey = new Text();
    //创建value的对象
    private FlowBean outValue = new FlowBean();

    /**
     * 在该方法中实现需要在MapTask中实现的功能
     * 注意：该方法在被循环调用每调用一次传入一行数据
     * @param key 读取的内容的偏移量
     * @param value 读取的内容（一行一行的内容）
     * @param context 上下文（在这用来将key,value写出去）
     * @throws IOException
     * @throws InterruptedException
     *
     * 1	13736230513	192.196.100.1	www.itis.com	2481	24681	200
     * 20 	13768778790	192.168.100.17			        120	      120	200
     */
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //1.切割数据
        String[] flowInfo = value.toString().split("\t");

        //2.封装key,value
        //给key赋值
        outKey.set(flowInfo[1]);
        //给value赋值
        outValue.setUpFlow(Long.parseLong(flowInfo[flowInfo.length-3]));
        outValue.setDownFlow(Long.parseLong(flowInfo[flowInfo.length-2]));
        outValue.setSumFlow(outValue.getUpFlow()+ outValue.getDownFlow());
//        outValue.setSumFlow(Long.parseLong(flowInfo[flowInfo.length-3])
//                + Long.parseLong(flowInfo[flowInfo.length-2]));

        //3.将key,value写出去
        context.write(outKey,outValue);

    }
}
