package com.itis.mrpractice.topn_61;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PracticeReducer extends Reducer<OrderBean, NullWritable,OrderBean, NullWritable> {
    private int i = 0;
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        if(i>=10) return;
        for (NullWritable value : values) {
            context.write(key,value);
            i++;
        }
    }
}
