package com.itis.mrpractice.topn_61;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PracticeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJobName("TopN");

        job.setJarByClass(PracticeDriver.class);
        job.setMapperClass(PracticeMapper.class);
        job.setReducerClass(PracticeReducer.class);
        job.setCombinerClass(PracticeReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputKeyClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("input"));
        FileOutputFormat.setOutputPath(job,new Path("output/practice"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
