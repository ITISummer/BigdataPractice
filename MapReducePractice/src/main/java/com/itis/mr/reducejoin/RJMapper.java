package com.itis.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RJMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private String fileName;//文件的名字
    //创建key的对象
    private OrderBean outKey = new OrderBean();

    /*
        在任务开始的时候执行一次，在map方法执行前执行
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //InputSplit : 切片对象（切片是FileSplit的对象，FileSplit是InputSplit的子类）
        InputSplit inputSplit = context.getInputSplit();
        //向下转型
        FileSplit fs = (FileSplit) inputSplit;
        //获取文件的名字
        fileName = fs.getPath().getName();
    }

    /*
            pd.txt
            01	小米
            02  华为

            order.txt
            1001	01	1
            1002    02  2
         */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //1.切割数据
        String[] line = value.toString().split("\t");
        //2.封装key,value
        if ("pd.txt".equals(fileName)){
            outKey.setPid(Long.parseLong(line[0]));
            outKey.setPname(line[1]);
        }else if("order.txt".equals(fileName)){
            outKey.setId(Long.parseLong(line[0]));
            outKey.setPid(Long.parseLong(line[1]));
            outKey.setAmount(Long.parseLong(line[2]));
            outKey.setPname("");//因数String默认值是null而且要参数排序
        }
        //3.将key,value写出去
        context.write(outKey,NullWritable.get());
    }
}
