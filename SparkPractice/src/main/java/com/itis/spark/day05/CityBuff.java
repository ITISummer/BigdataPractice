package com.itis.spark.day05;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CityBuff implements Serializable {

    //所有城市总的点击次数
    private Long totalCount;

    //每个城市的点击次数
    private Map<String,Long> cityCount;

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    public Map<String, Long> getCityCount() {
        return cityCount;
    }

    public void setCityCount(Map<String, Long> cityCount) {
        this.cityCount = cityCount;
    }
}
