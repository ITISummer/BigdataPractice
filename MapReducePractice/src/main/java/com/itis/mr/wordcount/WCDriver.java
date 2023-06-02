package com.itis.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    程序的入口
    1.创建Job对象
    2.给Job赋值
    3.提交Job

    常见错误：
        1.ClassCastException
           ①导包错误 -- 正确的org.apache.hadoop.io.Text 导成了javax.xml.soap
           ②设置错误：两个都是key的类型
                 job.setMapOutputKeyClass(Text.class);
                 job.setMapOutputKeyClass(LongWritable.class);

        2.不要在map和reduce方法中调用父类中的方法:super.map()或super.reduce

        3.NativeIOException:
            ①代码错误---用龙哥的代码跑一次
            ②创建org.apache.hadoop.io.nativeio包并将NativeIO.java放入到包里
            ③在windows上配置hadoop环境时是否把hadoop.dll和winutils.exe放入到c盘
            ④查看windows用户是否是中文或有空格（如果有重新创建windows用户）

        4.如果执行完wordcount后没有结果-文件里没有内容  那么一定是代码出错了
 */
public class WCDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1.创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.给Job赋值
        //2.1关联本Driver程序的jar
        job.setJarByClass(WCDriver.class);//如果是本地模式可以不写
        //2.2设置Mapper和Reducer类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        //2.3设置Mapper输出的key,value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //2.4设置最终输出的key,value的类型--(在这是Reducer输出的key,value的类型)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //2.5设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\io\\input"));
        //输出的路径一定不能存在否则报错
        FileOutputFormat.setOutputPath(job,new Path("D:\\io\\output23"));

        // 3.提交Job
        /*
        boolean waitForCompletion(boolean verbose)
        verbose :是否打印进度
        返回值：如果执行成功返回true
         */
        boolean b = job.waitForCompletion(true);
        System.out.println("b==" + b);
    }
}
