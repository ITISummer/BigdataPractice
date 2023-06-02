package com.itis.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
   向集群提交Job
   1.修改输入和输出的路径（从main方法中的参数中获取）
   2.打jar包
   3.将jar包放到集群中的任意一个节点上即可
   4.执行job : hadoop jar xxx.jar 全类名 /输入路径 /输出路径
 */
public class WCDriver2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1.创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.给Job赋值
        //2.1关联本Driver程序的jar
        job.setJarByClass(WCDriver2.class);//如果是本地模式可以不写
        //2.2设置Mapper和Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        //2.3设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //2.4设置最终输出的key,value的类型--(在这是Reducer输出的key,value的类型)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //2.5设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //输出的路径一定不能存在否则报错
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 3.提交Job
        /*
        boolean waitForCompletion(boolean verbose)
        verbose :是否打印进度
        返回值：如果执行成功返回true
         */
        boolean b = job.waitForCompletion(true);
        System.out.println("b==" + b);
    }
}
