package com.itis.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.Text;

public class MyPartitioner2 extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String key = text.toString();
        if (key.charAt(0) > 'a' && key.charAt(0) < 'k') {
            return 0;
        }
        return 1;
    }
}
