package com.itis.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建Job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        //2.Job设置参数
        //设置自定义OutputFormat---默认是TextOutputFormat
        job.setOutputFormatClass(MyOuputFormat.class);
        job.setJarByClass(OutDriver.class);
        job.setMapperClass(OutMapper.class);
        job.setReducerClass(OutReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input6"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output6"));

        //3.执行Job
        job.waitForCompletion(true);
    }
}
