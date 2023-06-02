package com.itis.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
   CombineTextInputFormat : 用于小文件过多的场景
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //1.设置虚拟存储切片最大值
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);//4m
        //2.默认是TextInputFormat(以文件单位)--设置CombineTextInputFormat
        //job.setInputFormatClass(CombineTextInputFormat.class);

        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input4"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output44"));


        boolean b = job.waitForCompletion(true);
        System.out.println("b==" + b);
    }
}
