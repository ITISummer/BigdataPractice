package com.itis.mrpractice.exec;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SaleBean implements WritableComparable<SaleBean> {
    private String brand;
    private Date sst;
    private Date edt;
    private String sst_new;

    private static final SimpleDateFormat sdf = new SimpleDateFormat( "G yyyy-MM-dd");

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public Date getSst() {
        return sst;
    }

    public void setSst(String sst) {
        try {
            this.sst = sdf.parse(sst);
        } catch (ParseException e) {
            System.out.println("设置 sst 错误！");
        }
    }

    public Date getEdt() {
        return edt;
    }

    public void setEdt(String edt) {

        try {
            this.edt = sdf.parse(edt);
        } catch (ParseException e) {
            System.out.println("设置 edt 错误！");
        }
    }

    public String getSst_new() {
        return sst_new;
    }

    public void setSst_new(String sst_new) {
        this.sst_new = sst_new;
    }

    @Override
    public int compareTo(SaleBean s) {
        return this.sst.compareTo(s.getSst());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(brand);
        dataOutput.writeUTF(sdf.format(sst));
        dataOutput.writeUTF(sdf.format(edt));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.brand = dataInput.readUTF();
        try{
            this.sst = sdf.parse(dataInput.readUTF());
            this.edt = sdf.parse(dataInput.readUTF());
        }catch(ParseException e) {
            System.out.println("日期转换错误！");
        }
    }

    @Override
    public String toString() { return brand + "\t" + sst.toString() + "\t" + edt.toString() ;}
}
