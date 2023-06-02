package com.itis.mrpractice.grptopn_62;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserIdGroupComparator extends WritableComparator {
    protected UserIdGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getUserId().compareTo(ob.getUserId());
    }
}
