package com.itis.mr.comparable2;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
自定义分区
Partitioner<KEY, VALUE>
KEY : map写出的key的类型
VALUE : map写出的value的类型
 */
public class MyPartitioner extends Partitioner<FlowBean, Text> {
    /**
     * 获取分区号
     * @param flowBean Map写出的key
     * @param text map写出的value
     * @param numPartitions ReduceTask的数量
     * @return
     */
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        //判断手机号
        String phoneNumber = text.toString();
        if (phoneNumber.startsWith("136")){
            return 0;//必须是从0开始
        } else if (phoneNumber.startsWith("137")){
            return 1;
        }else if (phoneNumber.startsWith("138")){
            return 2;
        }else if (phoneNumber.startsWith("139")){
            return 3;
        }else{
            return 4;
        }
    }
}
