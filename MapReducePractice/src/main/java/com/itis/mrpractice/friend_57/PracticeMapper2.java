package com.itis.mrpractice.friend_57;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PracticeMapper2 extends Mapper<Text, Text, Text, Text> {
    private Text k = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] friends = value.toString().split(",");
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                k.set(friends[i] + "-" + friends[j]);
                context.write(k, key);
            }
        }
    }
}
