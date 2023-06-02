package com.itis.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;
    public MyRecordWriter(TaskAttemptContext job) {
        try {
            //1.创建流-HDFS提供的流
            //1.1创建FileSystem对象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //1.2获取输出路径
            Path outputPath = FileOutputFormat.getOutputPath(job);
            //1.3获取输出流
            atguigu = fs.create(new Path(outputPath,"itis.txt"));
            other = fs.create(new Path(outputPath,"other.txt"));
        }catch (Exception e){ //不要关流因为下面要用
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
    /*
            将数据写出去
            注意：该方法在被循环调用，每调用一次传入一对k,v
    */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
//1.判断网址中是否包含atguigu
        String address = key.toString() + "\n";
        if (address.contains("itis")){//写到atguigu.txt
            atguigu.write(address.getBytes());//将字符串转成byte[]
        }else{//写到other.txt
            other.write(address.getBytes());
        }
    }
    /*
            关闭资源
            该方法是在write方法全部执行完毕后被调用的
    */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //IOUtils :Hadoop中提供的IO工具类
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
