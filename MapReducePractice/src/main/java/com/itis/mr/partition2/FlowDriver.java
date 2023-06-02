package com.itis.mr.partition2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    自定义分区
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());

        /*
        默认分区不需要考虑
            有几个RedcueTask就会有几个分区

        自定义分区时要考虑
            默认： ReduceTask的数量 = 分区的数量
                  ReduceTask的数量 < 分区的数量 : 报错
                  ReduceTask的数量 > 分区的数量 : 浪费资源
         */
        //设置ReduceTask的数量
        job.setNumReduceTasks(6);
        //设置使用自定义分区类
        job.setPartitionerClass(MyPartitioner.class);

        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input2"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output333"));

        job.waitForCompletion(true);
    }
}
