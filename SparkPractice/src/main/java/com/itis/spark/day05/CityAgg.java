package com.itis.spark.day05;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.*;

public class CityAgg extends Aggregator< String, CityBuff, String> {

    /**
     * 给中间变量赋予初始值
     * @return
     */
    public CityBuff zero() {
        CityBuff buff = new CityBuff();
        buff.setTotalCount(0L);
        buff.setCityCount(new HashMap<String, Long>());
        return buff;
    }

    /**
     * combiner预聚合
     * @param buff
     * @param city
     * @return
     */
    public CityBuff reduce( CityBuff buff,  String city) {
        //总次数+1
        buff.setTotalCount( buff.getTotalCount() + 1 );
        Map<String, Long> cityCount = buff.getCityCount();
        //判断城市是否之前点击过
        if(cityCount.containsKey(city)){
            //之前点击过,之前的该城市的点击次数 + 1
            cityCount.put(city, cityCount.get(city) + 1);
        }else{
            //没有点击,代表该城市第一次点击
            cityCount.put(city,1L);
        }
        return buff;
    }

    /**
     * 针对combiner结果汇总[reducer计算]
     * @param buff
     * @param combinerBuff
     * @return
     */
    public CityBuff merge( CityBuff buff,  CityBuff combinerBuff) {
        //buff = CityBuff( totalCount=28, cityCount=Map( 北京->3,天津->10,河南->5 ,山西->10) )
        //combierBuff = CityBuff( totalCount=12, cityCount=Map( 北京->2,天津->3,河北->5,广西->2) )
        //总点击次数
        //  28 + 12
        long totalCount = buff.getTotalCount() + combinerBuff.getTotalCount();
        buff.setTotalCount(totalCount);

        //Map( 北京->3,天津->10,河南->5 ,山西->10)
        Map<String, Long> cityCount = buff.getCityCount();
        //Map( 北京->2,天津->3,河北->5,广西->2)
        Map<String, Long> combierCityCount = combinerBuff.getCityCount();
        //每个城市的点击次数累加
        for(String city: combierCityCount.keySet()){
            //city=北京

            if( cityCount.containsKey(city) ){
                // 3  + 2
                long cc = cityCount.get(city) + combierCityCount.get(city);
                cityCount.put(city,cc);
            }else{
                cityCount.put(city, combierCityCount.get(city) );
            }
        }


        //Map(北京->5, 天津->13,河南->5,山西->10,河北->5,广西->2)
        return buff;
    }

    //最终计算
    public String finish( CityBuff buff) {
        //CityBuff( totalCount=40, cityCount=Map(北京->5, 天津->13,河南->5,山西->10,河北->5,广西->2) )
        //总点击次数
        Long totalCount = buff.getTotalCount();
        //每个城市的点击次数
        Map<String, Long> cityCount = buff.getCityCount();

        //对每个城市的点击数进行排序
        Set<Map.Entry<String, Long>> enty = cityCount.entrySet();
        ArrayList<Map.Entry<String, Long>> cityList = new ArrayList<Map.Entry<String, Long>>(enty);
        //降序排列
        Collections.sort(cityList, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (int)(o2.getValue() - o1.getValue()) ;
            }
        });

        String result = "";
        //判断城市的个数是否<=2个
        if(cityList.size()<=2) {

            for(Map.Entry<String, Long> en : cityList) {
                result = result + en.getKey()+":"+ (( en.getValue()+0.0) / totalCount) * 100 +"%,";
            }
            //去掉最后一个多余的,
            result = result.substring(0,result.length()-1);
        }else{

            //取出点击次数最多的两个
            List<Map.Entry<String, Long>> subList = cityList.subList(0, 2);

            Long top2CityCount = 0L;
            for(Map.Entry<String, Long> en : subList) {
                result = result + en.getKey()+":"+ (( en.getValue()+0.0) / totalCount) * 100 +"%,";
                top2CityCount = top2CityCount + en.getValue();
            }

            result = result + "其他:" + (((totalCount-top2CityCount)+0.0) / totalCount ) * 100 +"%";

        }

        return result;
    }

    //中间变量编码格式
    public Encoder<CityBuff> bufferEncoder() {
        return Encoders.bean(CityBuff.class);
    }

    //最终结果类型编码格式
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}
