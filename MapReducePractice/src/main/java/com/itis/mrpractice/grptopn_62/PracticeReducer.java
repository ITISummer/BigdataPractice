package com.itis.mrpractice.grptopn_62;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PracticeReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int idx = 0;
        for (NullWritable value : values) {
            context.write(key,value);
            idx++;
            if(idx>=3) break;
        }
    }
}
