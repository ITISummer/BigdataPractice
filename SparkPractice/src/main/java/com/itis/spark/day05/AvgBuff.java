package com.itis.spark.day05;

import java.io.Serializable;

public class AvgBuff implements Serializable {

    private Integer sum;

    private Integer count;

    public Integer getSum() {
        return sum;
    }

    public void setSum(Integer sum) {
        this.sum = sum;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
