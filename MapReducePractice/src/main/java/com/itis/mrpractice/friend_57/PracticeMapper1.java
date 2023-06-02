package com.itis.mrpractice.friend_57;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PracticeMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        v.set(split[0]);
        String[] split1 = split[1].split(",");
        for (String s : split1) {
            k.set(s);
            context.write(k, v);
        }
    }
}
