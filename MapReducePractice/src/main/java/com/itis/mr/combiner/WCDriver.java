package com.itis.mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
   Combiner : 对MapTask进行局部汇总 减少网络数据传输量
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1.创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置使用Combiner类
        job.setCombinerClass(WCCombiner.class);


        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input5"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output55"));


        boolean b = job.waitForCompletion(true);
        System.out.println("b==" + b);
    }
}
