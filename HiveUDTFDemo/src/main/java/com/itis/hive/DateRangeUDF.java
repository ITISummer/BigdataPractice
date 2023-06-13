package com.itis.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Pattern;

/*
[ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFSplit.java]
(https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDFSplit.java)
 */
public class DateRangeUDF extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters;
    private transient Pattern constPattern;

    //    定义日期格式化
    private SimpleDateFormat sdf;
    //    日历类，用于日期自增运算
    private Calendar dd;
    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        // 判断参数个数
        if (args.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function date_range(date1, date2) takes exactly 2 arguments.");
        }
        converters = new ObjectInspectorConverters.Converter[args.length];
        for (int i = 0; i < args.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(args[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        init();
        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                        .writableStringObjectInspector);
    }

    /**
     * 属性初始化
     */
    private void init() {
        sdf = new SimpleDateFormat("yyyy-MM-dd");
        dd = Calendar.getInstance();
    }
    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        assert (args.length == 2);

        if (args[0].get() == null || args[1].get() == null) {
            return null;
        }
        // 获取传入的参数
        Text dt1 = (Text) converters[0].convert(args[0].get());
        Text dt2 = (Text) converters[1].convert(args[1].get());

        ArrayList<Text> result = new ArrayList<>();

        // 日期格式化
        try {
            // 起始日期
            Date d1 = sdf.parse(dt1.toString());
            // 结束日期
            Date d2 = sdf.parse(dt2.toString());
            long d2Time = d2.getTime();
            Date tmp = d1;
            dd.setTime(d1);
            while (tmp.getTime() < d2Time) {
                tmp = dd.getTime();
                // 将输出数据放入输出数组的指定位置(每次写入时为覆盖写入),然后输出.本UDTF固定输出单列.
                result.add(new Text(sdf.format(tmp)));
                // 天数加上1
                dd.add(Calendar.DAY_OF_MONTH, 1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] args) {
        assert (args.length == 2);
        return getStandardDisplayString("date_range", args);
    }
}
