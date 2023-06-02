package com.itis.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
    自定义分组方式
    1.默认分组方式和排序方式相同
    2.自定义一个类继承WritableComparator
    3.①调用父类的构造器  ②重写compare(WritableComparable a, WritableComparable b) 方法注意形参类型
    4.在Driver中设置使用自定义分组方式
 */
public class MyComparator extends WritableComparator {

    //调用父类的构造器
    //WritableComparator(Class keyClass, boolean createInstances)
    // keyClass : key的运行时类      createInstances：是否创建实例（必须创建）
    public MyComparator(){
        super(OrderBean.class,true);
    }

    /*
        重写compare(WritableComparable a, WritableComparable b) 方法
            在该方法中指定分组的方式
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //向下转型
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) b;
        return Long.compare(o1.getPid(),o2.getPid());
    }
}
