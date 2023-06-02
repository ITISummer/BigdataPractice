package com.itis.mr.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer组件不能单独存在，因为Reducer要依赖于Mapper的输出。
     当引入了Reducer之后，最后输出的结果文件的结果就是Reducer的输出。
    1. Reducer组件用于接收Mapper组件的输出
    2. reduce的输入key,value需要和mapper的输出key,value类型保持一致
    3. reduce的输出key,value类型，根据具体业务决定
    4. reduce收到map的输出，会按相同的key做聚合，
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    private int sum;
    private IntWritable v = new IntWritable();

    /**
     *
     * @param key 与 mapper 输出的 key 类型保持一致
     * @param values mapper 输出的 value 类型一致
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        // 1 累加求和
        sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        // 2 输出
        v.set(sum);
        context.write(key,v);
    }
}
