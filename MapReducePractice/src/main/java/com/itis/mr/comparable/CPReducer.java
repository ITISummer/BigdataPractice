package com.itis.mr.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CPReducer extends Reducer<FlowBean, Text,Text,FlowBean> {

/*
    将读进来的Key,value 在写出去时交换位置
 */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            //将数据写出去
            context.write(value,key);
        }
    }
}
