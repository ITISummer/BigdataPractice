package com.itis.mrpractice.groupby_60;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PracticeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJobName("Group By");

        job.setJarByClass(PracticeDriver.class);
        job.setMapperClass(PracticeMapper.class);
        job.setReducerClass(PracticeReducer.class);
        // https://blog.csdn.net/qq_40315144/article/details/89372187
//        job.setCombinerClass(PracticeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(StatBean.class);

        FileInputFormat.setInputPaths(job,new Path("input"));
        FileOutputFormat.setOutputPath(job,new Path("output/practice"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
