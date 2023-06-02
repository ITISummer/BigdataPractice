package com.itis.mr.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RJReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    /*
                    key                         value
      （pd.txt）0     1     0    小米              null
   (order.txt)1001   1     1 　 ""                null        ====>  1001   1     1 　 "小米"
   (order.txt)1004   1     4    ""                null

        进行Join
        1.先获取第一条key的pname
        2.遍历剩下的所有数据
        3.将第一条的pname替换掉其它数据的pname
        4.将key,value写出去
     */
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //获取iterator迭代器--遍历数据
        Iterator<NullWritable> iterator = values.iterator();
        //next() ：①返回value的值  ②给key赋值（value所对应的key的值）
        NullWritable next = iterator.next();
        //1.先获取第一条key的pname
        String pname = key.getPname();
        //2.遍历剩下的所有数据
        while (iterator.hasNext()){
            iterator.next();
            //3.将第一条的pname替换掉其它数据的pname
            key.setPname(pname);
            //4.将key,value写出去
            context.write(key,NullWritable.get());
        }
    }
}
