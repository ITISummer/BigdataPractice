package com.itis.spark.day04;

import java.io.Serializable;
import java.util.Comparator;

public class UserActions implements Serializable, Comparable<UserActions> {

    private String id;

    private Integer clickNum;

    private Integer orderNum;

    private Integer payNum;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getClickNum() {
        return clickNum;
    }

    public void setClickNum(Integer clickNum) {
        this.clickNum = clickNum;
    }

    public Integer getOrderNum() {
        return orderNum;
    }

    public void setOrderNum(Integer orderNum) {
        this.orderNum = orderNum;
    }

    public Integer getPayNum() {
        return payNum;
    }

    public void setPayNum(Integer payNum) {
        this.payNum = payNum;
    }

    public int compareTo(UserActions o) {

        if( this.clickNum < o.getClickNum() ) {
            return -1;
        }else if( this.clickNum ==  o.getClickNum() ){

            if( this.orderNum < o.getOrderNum() ){
                return -1;
            }else if(this.orderNum == o.getOrderNum()){

                if(this.payNum < o.getPayNum()){
                    return -1;
                }else if(this.payNum == o.getPayNum()){
                    return 0;
                }else {
                    return 1;
                }
            }else {
                return 1;
            }

        }else {
            return 1;
        }

    }

    @Override
    public String toString() {
        return "id="+id+",点击次数="+clickNum+",下单次数="+orderNum+",支付次数="+payNum;
    }
}
