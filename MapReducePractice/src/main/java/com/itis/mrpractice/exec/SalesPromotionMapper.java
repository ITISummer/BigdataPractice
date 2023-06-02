package com.itis.mrpractice.exec;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SalesPromotionMapper extends Mapper<LongWritable, Text, Text, Text>{

    private final Text k = new Text();
    private final Text v = new Text();

    private final Map<String, List<SaleBean>> map = new HashMap<>();
    private List<SaleBean> list = new ArrayList<>();
    SaleBean s = new SaleBean();
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        k.set(split[0]);
        s.setBrand(split[0]);
        s.setSst(split[1]);
        s.setEdt(split[2]);
        if(map.get(split[0])==null){
            list.clear();
            list.add(s);
            map.put(split[0], list);
        } else {
            list = map.get(s.getBrand());
            list.add(s);
        }
//        context.write(k, v);
    }
}
