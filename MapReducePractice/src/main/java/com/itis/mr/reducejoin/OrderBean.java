package com.itis.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
    JavaBean
    该类要作为MR中的key所以需要实现WritableComparable接口
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private long id;
    private long pid;
    private long amount;
    private String pname;

    public OrderBean() {
    }

    /*
        先按pid排序再按pname排序
     */
    @Override
    public int compareTo(OrderBean o) {
        //先按pid排序
        int cpid = Long.compare(this.pid, o.pid);
        if (cpid == 0){//说明pid相同再按pname排序
            return -this.pname.compareTo(o.pname);
        }
        return cpid;
    }

    /*
        序列化时调用此方法
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeLong(pid);
        dataOutput.writeLong(amount);
        dataOutput.writeUTF(pname);
    }

    /*
        反序列化时调用此方法
        注意：反序列化时和序列化时的顺序要保持一致
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        pid = dataInput.readLong();
        amount = dataInput.readLong();
        pname = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getPid() {
        return pid;
    }

    public void setPid(long pid) {
        this.pid = pid;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }
}
