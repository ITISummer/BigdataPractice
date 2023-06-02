package com.itis.mr.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RJDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建Job实例
        Job job = Job.getInstance(new Configuration());

        //2.给Job赋值
        //设置使用自定义分组方式（不指定和排序方式相同）
        //job.setJarByClass(RJDriver.class);//如果是本地可以不用配置
        job.setGroupingComparatorClass(MyComparator.class);
        job.setMapperClass(RJMapper.class);
        job.setReducerClass(RJReducer.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input7"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output77"));

        //3.提交Job
        job.waitForCompletion(true);
    }
}
