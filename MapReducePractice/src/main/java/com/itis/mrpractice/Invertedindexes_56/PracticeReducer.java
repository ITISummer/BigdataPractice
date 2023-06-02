package com.itis.mrpractice.Invertedindexes_56;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PracticeReducer extends Reducer<Text, Text,Text, Text> {
    private Text v = new Text();
    private StringBuilder sb = new StringBuilder();
    private List<String> list = new ArrayList<>(3);
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        list.clear();
        // 添加到 list 中以便于排序
        for (Text s : values) {
            list.add(s.toString());
        }
        list.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                String[] value1 = o1.split(":");
                String[] value2 = o2.split(":");
                int compare = new Integer(value1[1]).compareTo(new Integer(value2[1]));
                return ( compare != 0 ? -compare: value1[0].compareTo(value2[0]));
            }
        });
        sb.setLength(0);
        for (String s : list) {
            sb.append(s);
            sb.append(",");
        }
//         去除最后一个 ','
        v.set(sb.substring(0,sb.length()-1));
        context.write(key,v);
    }
}
