package com.itis.mrpractice.groupby_60;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PracticeMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        k.set(words[1]);
        v.set(Integer.parseInt(words[2]));
        context.write(k,v);
    }
}
