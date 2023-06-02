package com.itis.mrpractice.join_59;

import org.apache.commons.lang.StringUtils;
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

public class PracticeMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private Map<String, String> userMap = new HashMap<>();
    private OrderBean v = new OrderBean();

    //任务开始前将pd数据缓存进pdMap
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // 通过缓存文件得到小表数据 user.csv
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //通过包装流转换为reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割一行
            String[] split = line.split(",");
            userMap.put(split[0],split[1]);
        }
        userMap.put("1", "margarete@example.org");
        //关流
        IOUtils.closeStream(reader);
        IOUtils.closeStream(fis);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取大表数据
        String[] fields = value.toString().split(",");
        v.setOrderId(fields[0]);
        v.setUserId(fields[1]);
        //通过大表每行数据的user_id,去pdMap里面取出email
        v.setEmail(userMap.get(fields[1]));
        v.setAmount(Integer.parseInt(fields[2]));
        //写出
        context.write(v, NullWritable.get());
    }
}