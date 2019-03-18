/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jaqu;

import static org.h2.jaqu.Define.primaryKey;
import static org.h2.jaqu.Define.tableName;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.h2.jaqu.Table;

/**
 * A table containing order data.
 */

public class Order implements Table {
    public String customerId;
    public Integer orderId;
    public Date orderDate;
    public BigDecimal total;

    public Order(String customerId, Integer orderId,
            String total, String orderDate) {
        this.customerId = customerId;
        this.orderId = orderId;
        this.total = new BigDecimal(total);
        this.orderDate = java.sql.Date.valueOf(orderDate);
    }

    public Order() {
        // public constructor
    }

    @Override
    public void define() {
        tableName("Orders");
        primaryKey(customerId, orderId);
    }

    public static List<Order> getList() {
        Order[] list = {
                new Order("ALFKI", 10702, "330.00", "2007-01-02"),
                new Order("ALFKI", 10952, "471.20", "2007-02-03"),
                new Order("ANATR", 10308, "88.80", "2007-01-03"),
                new Order("ANATR", 10625, "479.75", "2007-03-03"),
                new Order("ANATR", 10759, "320.00", "2007-04-01"),
                new Order("ANTON", 10365, "403.20", "2007-02-13"),
                new Order("ANTON", 10682, "375.50", "2007-03-13"),
                new Order("ANTON", 10355, "480.00", "2007-04-11") };
        return Arrays.asList(list);
    }

}
