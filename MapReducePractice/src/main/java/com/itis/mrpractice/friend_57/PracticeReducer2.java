package com.itis.mrpractice.friend_57;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PracticeReducer2 extends Reducer<Text, Text,Text, Text> {
    StringBuffer sb = new StringBuffer();
    private Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.setLength(0);
        List<String> outputTemp = new ArrayList<>();
        values.forEach(text -> outputTemp.add(text.toString()));
        outputTemp.stream().sorted().forEach(str -> sb.append(str).append(','));
        v.set(sb.substring(0, sb.length() - 1));
        context.write(key, v);
    }
}
