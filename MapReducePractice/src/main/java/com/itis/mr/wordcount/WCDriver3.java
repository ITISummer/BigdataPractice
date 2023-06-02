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
   从本地向集群提交(了解即可)
   1.配置参数
         //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //指定MR运行在Yarn上
        conf.set("mr.framework.name","yarn");
        //指定MR可以在远程集群运行
        conf.set("mr.app-submission.cross-platform", "true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname", "hadoop103");
   2.从main方法的参数读取输入和输出路径
   3.打jar包
   4.注释掉 //job.setJarByClass(WCDriver3.class);//如果是本地模式可以不写
     添加如下代码 ：job.setJar("jar包路径");
   5.
     VM OPtions : -DHADOOP_USER_NAME=itis
     ProgramArguments : hdfs://hadoop102:8020/input hdfs://hadoop102:8020/output
 */
public class WCDriver3 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1.创建Job对象
        Configuration conf = new Configuration();
        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //指定MR运行在Yarn上
        conf.set("mr.framework.name","yarn");
        //指定MR可以在远程集群运行
        conf.set("mr.app-submission.cross-platform", "true");
        //指定yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname", "hadoop103");

        Job job = Job.getInstance(conf);

        // 2.给Job赋值
        //2.1关联本Driver程序的jar
        //job.setJarByClass(WCDriver3.class);//如果是本地模式可以不写

        //设置jar包的路径
        job.setJar("D:\\class_video\\230201\\07-Hadoop\\3.代码\\MRDemo\\target\\MRDemo-1.0-SNAPSHOT.jar");
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.out.println("b==" + b);
    }
}
