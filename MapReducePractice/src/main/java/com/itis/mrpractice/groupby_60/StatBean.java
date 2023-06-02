package com.itis.mrpractice.groupby_60;

public class StatBean {
    private int total;
    private int max;
    private int min;
    private int count;

    public StatBean() {
        this.max = Integer.MIN_VALUE;
        this.min = Integer.MAX_VALUE;
        this.total = 0;
        this.count = 0;
    }
    /**
     * 添加并统计
     * 更新最大值
     * 更新最小值
     * 计算总数
     * 计算数量 count
     */
    public void putAndStatistic(int count) {
        this.total+=count;
        this.count++;
        if(this.max<count) this.max = count;
        if(this.min>count) this.min = count;
    }
    public double getAvg() {
        return (double) total/count;
    }

    @Override
    public String toString() {
        // https://blog.csdn.net/qq_44111805/article/details/112850550
//        return this.total+"\t"+String.format("%.2f",this.getAvg())+"\t"+this.max+"\t"+this.min;
        return this.total+"\t"+this.getAvg()+"\t"+this.max+"\t"+this.min;
    }
}
