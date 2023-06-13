package com.itis.hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/*
[java中日期的循环](https://blog.csdn.net/weixin_41086086/article/details/88722622)
 */
public class Test{

    public static void main(String[] args) {
        // 日期格式化
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            // 起始日期
            Date d1 = sdf.parse("2018-2-25");
            // 结束日期
            Date d2 = sdf.parse("2018-3-5");
            Date tmp = d1;
            Calendar dd = Calendar.getInstance();
            dd.setTime(d1);
            // 打印2018年2月25日到2018年3月5日的日期
            while (tmp.getTime() < d2.getTime()) {
                tmp = dd.getTime();
                System.out.println(sdf.format(tmp));
                // 天数加上1
                dd.add(Calendar.DAY_OF_MONTH, 1);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
