package com.itis.mrpractice.join_59;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class PracticeDriver{

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJobName("Join");

        // 加载缓存数据
        job.addCacheFile(new URI("file:///D:/CodingWorkplaces/IntellijIdeaWorkplaces/MapReduceDemo/input/user.csv"));
        // Map端Join的逻辑不需要Reduce阶段，设置 ReduceTask 数量为0
//        job.setNumReduceTasks(0);

        job.setJarByClass(PracticeDriver.class);
        job.setMapperClass(PracticeMapper.class);
        job.setReducerClass(PracticeReducer.class);
        job.setCombinerClass(PracticeReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputKeyClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path("input/order.csv"));
        FileOutputFormat.setOutputPath(job,new Path("output/practice"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
