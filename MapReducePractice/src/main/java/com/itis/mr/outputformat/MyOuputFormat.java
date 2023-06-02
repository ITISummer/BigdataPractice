package com.itis.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    自定义的OutputFormat
    问题：继承谁？

    FileOutputFormat<K, V> :
        K : reduce写出的key的类型
        V : reduce写出的valur的类型
 */
public class MyOuputFormat extends FileOutputFormat<Text, NullWritable> {

    /*
            返回RecordWriter对象
            真正写数据是在RecordWriter里面执行的
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        return new MyRecordWriter(job);
    }
}
