package com.itis.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/*
参考：
[Hive自定义表生成函数UDTF的自定义实现Demo](https://blog.csdn.net/TomAndersen/article/details/106413589)
[Hive自定义UDTF函数](https://blog.csdn.net/QiSorry/article/details/114650015)
[[Hive]表生成函数(UDTF)使用指南](https://blog.csdn.net/kaizuidebanli/article/details/115191716)
[HIVE自定义UDTF函数](https://blog.csdn.net/weixin_46429290/article/details/126670800)
https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF
https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFSplit.java

 */
public class DateRangeUDTF extends GenericUDTF {
//    保存所有输入字段对应的 ObjectInspector
    private transient ObjectInspector[] inputFields;
//    保存输出列个数
    private int numCols;
//    保存输出列
    private transient ArrayList<String> resCols;
//    保存空输出列,用于在特殊情况输出
    private transient Object[] nullCols;
//    定义日期格式化
    SimpleDateFormat sdf;
//    日历类，用于日期自增运算
    Calendar dd;

    /**
     * 此方法一般用于检查输入参数是否合法,每个实例只会调用一次.
     * <p>
     * 一般用于用于检查UDTF函数的输入参数,以及生成一个用于查看新生成字段数据的StructObjectInspector.
     *
     * @param argOIs {@link StructObjectInspector},传入的是所有输入参数字段的查看器
     * @return {@link StructObjectInspector},返回的是新生成字段的查看器
     * @date 2023年5月10日
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1.输入参数检查
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();// 获取所有的输入参数字段
        int argsLength = inputFields.size();

        if (argsLength != 2) {// 检查参数个数
            throw new UDFArgumentException("date_range(date1,date2) 需要两个日期字符串参数");
        }

        this.inputFields = new ObjectInspector[argsLength];// 获取所有输入参数字段对应的检查器(Inspector)
        for (int i = 0; i < argsLength; i++) {
            this.inputFields[i] = inputFields.get(i).getFieldObjectInspector();
        }

        for (int i = 0; i < argsLength; ++i) {// 检查参数类型
            if (!this.inputFields[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME) ||
                    this.inputFields[i].getCategory() != ObjectInspector.Category.PRIMITIVE
            ) {
                throw new UDFArgumentException("date_range(date1,date2) 的参数必须是日期字符串类型");
            }
        }

        // 2.成员变量赋值
        init();

        // 3.定义输出字段的列名(字段名)
        // 此处设置的各个列名很可能会在用户执行Hive SQL查询时,会被其指定的列别名所覆盖,如果查询时未指定别名,则显示此列名.
        // 此自定义表生成函数UDTF只输出单列,所以只添加了单个列的列名
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("explode_dt");

        // 4.定义输出字段对应字段类型的ObjectInspector(对象检查器),一般用于查看/转换/操控该字段的内存对象
        // 只是查看器,并非是真正的字段对象
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        // 由于输出字段的数据类型都是String类型
        // 因此使用检查器PrimitiveObjectInspectorFactory.javaStringObjectInspector
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 5.生成并返回输出对象的对象检查器
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 成员变量赋值
     */
    private void init() {
        numCols = 1;
        resCols = new ArrayList<>();
        nullCols = new Object[numCols];
        sdf = new SimpleDateFormat("yyyy-MM-dd");
        dd = Calendar.getInstance();
    }

    /**
     * 此方法为核心处理方法,声明主要的处理过程,如果是对表使用函数,则会对表中每一行数据通过同一个实例调用一次此方法.
     * 输入参数为待处理的字符串以及分隔符正则表达式,每次处理完后直接使用forward方法将数据传给收集器.
     */
    @Override
    public void process(Object[] args) throws HiveException {
        // 1. 通过 ObjectInspector 获取待处理字符串
        String dt1 = ((StringObjectInspector) inputFields[0]).getPrimitiveJavaObject(args[0]);
        String dt2 = ((StringObjectInspector) inputFields[1]).getPrimitiveJavaObject(args[1]);
        // 如果对象为null则直接输出空行
        if ((dt1 == null || dt1.length() == 0) || (dt2 == null || dt2.length() == 0)) {
            forward(nullCols);
        }

        // 日期格式化
        try {
            // 起始日期
            Date d1 = sdf.parse(dt1);
            // 结束日期
            Date d2 = sdf.parse(dt2);
            long d2Time = d2.getTime();
            Date tmp = d1;
            dd.setTime(d1);
            while (tmp.getTime() < d2Time) {
                tmp = dd.getTime();
                // 将输出数据放入输出数组的指定位置(每次写入时为覆盖写入),然后输出.本UDTF固定输出单列.
//                resCols.add(sdf.format(tmp));
                forward(sdf.format(tmp));
                // 天数加上1
                dd.add(Calendar.DAY_OF_MONTH, 1);
            }
//            forward(resCols);
//            resCols.clear();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回此UDTF对应的函数名
     */
    @Override
    public String toString() {
        return "date_range";
    }

    /**
     * 释放相关资源，一般不需要
     */
    @Override
    public void close() throws HiveException {

    }
}
