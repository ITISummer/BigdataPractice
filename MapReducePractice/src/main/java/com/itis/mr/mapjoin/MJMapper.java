package com.itis.mr.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/*
    MapJoin的思路
    1.先将小文件缓存到内存中
    2.map方法读取大文件中的数据（一行一行读）
    3.再通过缓存获取对应的数据并拼接（join_59）在当前数据
    4.将数据写出去
 */
public class MJMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //缓存数据: key是pid, value是pname
    private Map<String,String> map = new HashMap<>();
    /*
        将小文件缓存到内存中
        该方法在任务开始时执行只执行一次（在map方法前执行）
        作用：初始化
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        BufferedReader br = null;
        try {
            //1.读取文件中的数据
            //1.1创建FileSystem对象
            FileSystem fs = FileSystem.get(context.getConfiguration());
            //1.2创建流
            //1.2.1获取缓存文件的路径
            URI[] cacheFiles = context.getCacheFiles();
            //1.2.2创建流
            FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
            //1.2.3我们需要一行一行读取数据字符缓冲流，需要使用转换流将字节流转成字符流
            br = new BufferedReader(new InputStreamReader(fis));
            //1.2.4读取数据
            String line = "";
            while ((line = br.readLine()) != null) {
                //2.将读取的数据放入到map中
                String[] split = line.split("\t");
                map.put(split[0], split[1]);
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }finally {
            IOUtils.closeStream(br);
        }

    }

    /*
            map里：
                01 ， 小米
                02 ， 华为

            在该方法中进行join
            1001	01	1
            1002	02	2
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        //获取map中对应的value
        String pname = map.get(line[1]);
        //字符串拼接
        String s = line[0] + "\t" + pname + "\t" + line[2];
        //封装key,value
        Text outKey = new Text();
        outKey.set(s);
        //将key,value写出去
        context.write(outKey,NullWritable.get());
    }

    /*
            该方法在任务结束时执行只执行一次（在map方法后执行）
            作用：关闭资源
    */
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

    }
}
