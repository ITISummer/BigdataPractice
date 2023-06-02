package com.itis.mrpractice.join_59;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PracticeReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    private final List<OrderBean> orderList = new ArrayList<>();
    private final List<OrderBean> userList = new ArrayList<>();
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key,value.get());
        }
    }
}
