package com.itis.mr.writable2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1.创建Job对象
        Job job = Job.getInstance(new Configuration());

        // 2.给Job赋值
        //2.1关联本Driver程序的jar
        job.setJarByClass(FlowDriver.class);
        //2.2设置Mapper和Reducer类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //2.3设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //2.4设置最终输出的key,value的类型--(在这是Reducer输出的key,value的类型)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //2.5设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input2"));
        //输出的路径一定不能存在否则报错
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output22"));
        // 3.提交Job
        /*
        boolean waitForCompletion(boolean verbose)
        verbose :是否打印进度
        返回值：如果执行成功返回true
         */
        job.waitForCompletion(true);
    }
}
