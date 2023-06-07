package com.itis.spark.day05;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * 自定义UDAF函数
 *      1、创建一个class继承Aggregator<IN,BUFF,OUT>
 *              IN: 代表UDAF参数类型
 *              BUFF: 代表计算过程的中间结果类型
 *              OUT: 代表UDAF返回值类型
 *      2、重新抽象方法
 *      3、创建自定义UDAF对象
 *      4、通过functions.udaf转换类型
 *      5、注册udaf函数
 *      6、sql中使用
 */
public class AvgAgg extends Aggregator<Integer,AvgBuff,Double> {

    /**
     * 给中间变量赋予初始值
     * @return
     */
    public AvgBuff zero() {
        AvgBuff buff = new AvgBuff();
        buff.setSum(0);
        buff.setCount(0);
        return buff;
    }

    /**
     * 预聚合[combiner]
     * @param buff 中间变量
     * @param score 待计算的元素
     * @return
     */
    public AvgBuff reduce(AvgBuff buff, Integer score) {

        //统计总成绩
        buff.setSum( buff.getSum() + score );
        //统计格式
        buff.setCount( buff.getCount() + 1);

        return buff;
    }

    /**
     * reducer计算逻辑[对多个分区combiner结果汇总]
     * @param buff 中间变量
     * @param combinerBuff 待汇总的combiner结果
     * @return
     */
    public AvgBuff merge( AvgBuff buff, AvgBuff combinerBuff) {
        //汇总总分
        buff.setSum( buff.getSum() + combinerBuff.getSum() );
        //汇总总个数
        buff.setCount( buff.getCount() + combinerBuff.getCount() );

        return buff;
    }

    /**
     * 返回最终结果
     * @param buff
     * @return
     */
    public Double finish( AvgBuff buff) {
        //求得平均分
        return (buff.getSum()+0.0) / buff.getCount();
    }

    /**
     * 指定中间变量的序列化编码格式
     * @return
     */
    public Encoder<AvgBuff> bufferEncoder() {
        return Encoders.bean(AvgBuff.class);
    }

    /**
     * 指定最终结果类型的序列化编码格式
     * @return
     */
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }
}
