package com.itis.mrpractice.topn_61;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PracticeMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        k.setOrderId(words[0]);
        k.setUserId(words[1]);
        k.setAmount(Integer.parseInt(words[2]));
        context.write(k,NullWritable.get());
    }
}
