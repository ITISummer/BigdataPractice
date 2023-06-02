package com.itis.mrpractice.topn_61;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private String userId;
    private int amount;

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public int compareTo(OrderBean o) {
        return Integer.compare(o.amount, this.amount);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(userId);
        out.writeInt(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.userId = in.readUTF();
        this.amount = in.readInt();
    }
    @Override
    public String toString() {
        return orderId + "\t" + userId + "\t" + amount;
    }
}
