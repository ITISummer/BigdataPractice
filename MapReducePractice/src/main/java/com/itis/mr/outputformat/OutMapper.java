package com.itis.mr.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
    在value的位置写的NullWritable
    NullWritable表示没有value
 */
public class OutMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        NullWritable nullWritable = NullWritable.get();
        //将数据写出去
        context.write(value,nullWritable);
    }
}
