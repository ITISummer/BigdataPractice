package com.itis.mr.comparable2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CPDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.创建Job
        Job job = Job.getInstance(new Configuration());

        //设置ReduceTask的数量
        job.setNumReduceTasks(5);
        //设置使用自定义分区类
        job.setPartitionerClass(MyPartitioner.class);

        //2.给Job赋值
        job.setJobName("longge");
        job.setJarByClass(CPDriver.class);
        job.setMapperClass(CPMapper.class);
        job.setReducerClass(CPReducer.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input3"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output333"));

        //3.提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);//退出JVM--0正常退出其它非正常退出
    }
}
