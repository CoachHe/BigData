package com.coachhe.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id;       //订单ID

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    private double price;       //订单价格

    public OrderBean(){
        super();
    }

    public OrderBean(int order_id, double price) {
        super();
        this.order_id = order_id;
        this.price = price;
    }

    public int compareTo(OrderBean o) {
        // 先按照订单id进行排序，如果相同按照价格降序排序
        int res = 0;
        if (order_id > o.getOrder_id()) {
            res = 1;
        } else if (order_id < o.getOrder_id()) {
            res = -1;
        } else {
            if (price > o.getPrice()) {
                res = -1;
            } else if (price < o.getPrice()) {
                res = 1;
            } else {
                res = 0;
            }
        }
        return res;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "order_id=" + order_id + "\t" + "price=" + price;
    }
}
