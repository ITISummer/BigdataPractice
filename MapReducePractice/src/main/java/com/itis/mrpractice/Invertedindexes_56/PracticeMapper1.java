package com.itis.mrpractice.Invertedindexes_56;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class PracticeMapper1 extends Mapper<LongWritable, Text, Text,IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);
    private String filename;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        if (inputSplit instanceof FileSplit) {
            filename = ((FileSplit) inputSplit).getPath().getName();
        } else {
            filename = "";
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            k.set(word+","+filename);
            context.write(k,v);
        }
    }
}
