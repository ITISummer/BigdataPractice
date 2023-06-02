package com.itis.mrpractice.Invertedindexes_56;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PracticeReducer1 extends Reducer<Text, IntWritable,Text, IntWritable> {
    private IntWritable v = new IntWritable();
    private int sum;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for (IntWritable s : values) {
            sum+=s.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
