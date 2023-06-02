package com.itis.mrpractice.Invertedindexes_56;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
public class PracticeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // job1
        Job job1 = Job.getInstance(conf);
        job1.setMapperClass(PracticeMapper1.class);
        job1.setReducerClass(PracticeReducer1.class);
        job1.setCombinerClass(PracticeReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job1, new Path("input"));
        FileOutputFormat.setOutputPath(job1, new Path("output/practice_temp"));

        // job2
        Job job2 = Job.getInstance(conf);
        job2.setMapperClass(PracticeMapper.class);
        job2.setReducerClass(PracticeReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job2, new Path("output/practice_temp"));
        FileOutputFormat.setOutputPath(job2, new Path("output/practice"));

        // 序列化
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);

        System.exit(job1.waitForCompletion(true)  && job2.waitForCompletion(true) ? 0 : 1);
    }
}
