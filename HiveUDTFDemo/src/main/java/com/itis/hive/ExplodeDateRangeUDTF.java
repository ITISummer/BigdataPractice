package com.itis.hive;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
//import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * chatGPT 写的
 * 用不了
 */
public class ExplodeDateRangeUDTF extends GenericUDTF {

    private ObjectInspector stringOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException("date_range() takes exactly two arguments");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                || ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("date_range() first argument must be a string");
        }

        if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE
                || ((PrimitiveObjectInspector) args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("date_range() second argument must be a string");
        }

        // 初始化输出列名和列类型
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("date");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String startDate = ((StringObjectInspector) stringOI).getPrimitiveJavaObject(args[0]);
        String endDate = ((StringObjectInspector) stringOI).getPrimitiveJavaObject(args[1]);

        // 解析开始和结束日期
        LocalDate start = LocalDate.parse(startDate);
        LocalDate end = LocalDate.parse(endDate);

        // 输出日期范围中的每个日期
        for (LocalDate date = start; date.isBefore(end.plusDays(1)); date = date.plusDays(1)) {
            Text result = new Text(date.toString());
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}
