package com.itis.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OutReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    //一组
    /*
        www.baidu.com
        www.baidu.com
     */
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //直接将数据写出去
        for (NullWritable value : values) {
            context.write(key,value);
        }
    }
}
