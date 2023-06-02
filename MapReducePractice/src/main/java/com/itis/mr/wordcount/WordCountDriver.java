package com.itis.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置ReduceTask的数量
        job.setNumReduceTasks(2);
        //设置使用自定义分区类
        job.setPartitionerClass(MyPartitioner2.class);

        // 2 关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("D:\\hadoop_input_test\\hello.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output\\"));

        // 7 提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}


/**
 * 在 Windows 上向集群提交任务
 */
//public class WordCountDriver {
//
//	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//
//		// 1 获取配置信息以及封装任务
//		Configuration conf = new Configuration();
//
//       //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
//        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
//        //指定MR运行在Yarn上
//        conf.set("mr.framework.name","yarn");
//        //指定MR可以在远程集群运行
//        conf.set("mr.app-submission.cross-platform",
//"true");
//        //指定yarn resourcemanager的位置
//        conf.set("yarn.resourcemanager.hostname",
//"hadoop103");
//		Job job = Job.getInstance(conf);
//		// 2 设置jar加载路径
//		job.setJarByClass(WordCountDriver.class);
//
//		// 3 设置map和reduce类
//		job.setMapperClass(WordCountMapper.class);
//		job.setReducerClass(WordCountReducer.class);
//
//		// 4 设置map输出
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
//
//		// 5 设置最终输出kv类型
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//
//		// 6 设置输入和输出路径
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//		// 7 提交
//		boolean result = job.waitForCompletion(true);
//
//		System.exit(result ? 0 : 1);
//	}
//}
