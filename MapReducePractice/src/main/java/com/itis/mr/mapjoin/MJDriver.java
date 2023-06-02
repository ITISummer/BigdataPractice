package com.itis.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MJDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {

        //1.创建Job实例
        Job job = Job.getInstance(new Configuration());

        //2.给Job设置参数
        //添加缓存文件的路径
        job.addCacheFile(new URI("file:///D:/io/input7/pd.txt"));
        //如果ReduceTask设置为0 不排序
        job.setNumReduceTasks(0);

        job.setMapperClass(MJMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);//map的key
        job.setOutputValueClass(NullWritable.class);//map的value
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input7\\order.txt"));//map方法读取的数据的路径
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output7"));

        //3.执行Job
        job.waitForCompletion(true);
    }
}
