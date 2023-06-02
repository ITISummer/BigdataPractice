package com.itis.mrpractice.Invertedindexes_56;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PracticeMapper extends Mapper<Text, IntWritable, Text,Text> {
    private Text k = new Text();
    private Text v = new Text();
    private String filename;
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String[] words = key.toString().split(",");
        k.set(words[0]);
        v.set(words[1]+":"+ value.get());
        context.write(k,v);
    }
}
