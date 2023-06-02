package com.itis.mrpractice.groupby_60;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PracticeReducer extends Reducer<Text, IntWritable,Text,StatBean> {
    private StatBean v;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        v = new StatBean();
        for (IntWritable s : values) {
            v.putAndStatistic(s.get());
        }
        context.write(key,v);
    }
}
