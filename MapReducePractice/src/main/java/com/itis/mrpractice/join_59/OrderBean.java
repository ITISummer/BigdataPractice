package com.itis.mrpractice.join_59;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private String userId;
    private String email;
    private int amount;

    @Override
    public String toString() {
        return orderId + "\t" + userId + "\t" + email + "\t" + amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public int compareTo(OrderBean o) {
        String str1=String.valueOf(userId);
        String str2=String.valueOf(o.userId);
        if(str1.length()==str2.length()){
            //长度相同,升序
            return Integer.parseInt(userId)-Integer.parseInt(o.userId);
        }else{
            for(int i=0;i<Math.min(str1.length(),str2.length());i++){
                if(str1.charAt(i)!=str2.charAt(i)){
                    return str1.charAt(i)-str2.charAt(i);
                }
            }
            return str1.length()-str2.length();
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(email);
        dataOutput.writeInt(amount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.email = dataInput.readUTF();
        this.amount = dataInput.readInt();
    }
}
