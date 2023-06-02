package com.itis.mr.wordcount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 [详解HDFS的Mapper类和Reduce类及4个泛型参数含义](https://blog.csdn.net/weixin_43181007/article/details/87253430)
 泛型一:KEYIN：LongWritable，对应的Mapper的输入key。输入key是每行的行首偏移量
 泛型二:VALUEIN：Text，对应的Mapper的输入Value。输入value是每行的内容
 泛型三:KEYOUT：对应的Mapper的输出key，根据业务来定义，这里定义为 Text
 泛型四:VALUEOUT：对应的Mapper的输出value，根据业务来定义，这里定义为 IntWritable

 注意：初学时，KEYIN和VALUEIN写死(LongWritable,Text)。KEYOUT和VALUEOUT不固定,根据业务来定
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    // 输出的key类型
    private Text k = new Text();
    // 输出的value类型
    private IntWritable v = new IntWritable(1);

    /**
     *
     * @param key 每行的首行偏移量
     * @param value 一行的内容
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
