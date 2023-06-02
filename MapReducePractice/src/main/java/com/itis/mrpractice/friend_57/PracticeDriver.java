package com.itis.mrpractice.friend_57;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * [数据分析之共同好友统计](https://blog.51cto.com/u_15469043/4892025)
 */
public class PracticeDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // job1
        Job job1 = Job.getInstance(conf);
        job1.setMapperClass(PracticeMapper1.class);
        job1.setReducerClass(PracticeReducer1.class);
        job1.setCombinerClass(PracticeReducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job1, new Path("input/friends.txt"));
        FileOutputFormat.setOutputPath(job1, new Path("output/practice_temp"));

        // job2
        Job job2 = Job.getInstance(conf);
        job2.setMapperClass(PracticeMapper2.class);
        job2.setReducerClass(PracticeReducer2.class);
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
