package com.itis.mr.partition2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
    在该类中实现需要在ReduceTask中需要实现的功能

    自定一个类并继承 Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        KEYIN: 读取的key的类型（Mapper写出的key的类型）
        VALUEIN：读取的value的类型（Mapper写出的value的类型）

        KEYOUT ： 写出的key的类型（在这是手机号类型）
        VALUEOUT：写出的value的类型（在这是FlowBean类型）
 */
public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {
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
     *      Text            FlowBean
     * 1532121212       100   100   200     ======> 1532121212   300  200  500
     * 1532121212       200   100   300

     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;//总上行
        long sumDownFlow = 0;//总下行

        //1.遍历所有的value
        for (FlowBean value : values) {
            //2.对value中的数据进行累加
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        //3.封装key,value
        //创建value的对象
        FlowBean outValue = new FlowBean();
        //给value赋值
        outValue.setUpFlow(sumUpFlow);
        outValue.setDownFlow(sumDownFlow);
        outValue.setSumFlow(outValue.getUpFlow() +  outValue.getDownFlow());

        //4.将key,value写出去
        context.write(key,outValue);
    }
}
