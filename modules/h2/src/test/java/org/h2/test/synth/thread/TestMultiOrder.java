/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.thread;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * The operation part of {@link TestMulti}.
 * Queries and updates two tables.
 */
public class TestMultiOrder extends TestMultiThread {

    private static int customerCount;
    private static int orderCount;
    private static int orderLineCount;

    private static final String[] ITEMS = { "Apples", "Oranges",
            "Bananas", "Coffee" };

    private Connection conn;
    private PreparedStatement insertLine;

    TestMultiOrder(TestMulti base) throws SQLException {
        super(base);
        conn = base.getConnection();
    }

    @Override
    void begin() throws SQLException {
        insertLine = conn.prepareStatement("insert into orderLine" +
                "(order_id, line_id, text, amount) values(?, ?, ?, ?)");
        insertCustomer();
    }

    @Override
    void end() throws SQLException {
        conn.close();
    }

    @Override
    void operation() throws SQLException {
        if (random.nextInt(10) == 0) {
            insertCustomer();
        } else {
            insertOrder();
        }
    }

    private void insertOrder() throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "insert into orders(customer_id , total) values(?, ?)");
        prep.setInt(1, random.nextInt(getCustomerCount()));
        BigDecimal total = new BigDecimal("0");
        prep.setBigDecimal(2, total);
        prep.executeUpdate();
        ResultSet rs = prep.getGeneratedKeys();
        rs.next();
        int orderId = rs.getInt(1);
        int lines = random.nextInt(20);
        for (int i = 0; i < lines; i++) {
            insertLine.setInt(1, orderId);
            insertLine.setInt(2, i);
            insertLine.setString(3, ITEMS[random.nextInt(ITEMS.length)]);
            BigDecimal amount = new BigDecimal(
                    random.nextInt(100) + "." + random.nextInt(10));
            insertLine.setBigDecimal(4, amount);
            total = total.add(amount);
            insertLine.addBatch();
        }
        insertLine.executeBatch();
        increaseOrderLines(lines);
        prep = conn.prepareStatement(
                "update orders set total = ? where id = ?");
        prep.setBigDecimal(1, total);
        prep.setInt(2, orderId);
        increaseOrders();
        prep.execute();
    }

    private void insertCustomer() throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "insert into customer(id, name) values(?, ?)");
        int customerId = getNextCustomerId();
        prep.setInt(1, customerId);
        prep.setString(2, getString(customerId));
        prep.execute();
    }

    private static String getString(int id) {
        StringBuilder buff = new StringBuilder();
        Random rnd = new Random(id);
        int len = rnd.nextInt(40);
        for (int i = 0; i < len; i++) {
            String s = "bcdfghklmnprstwz";
            char c = s.charAt(rnd.nextInt(s.length()));
            buff.append(i == 0 ? Character.toUpperCase(c) : c);
            s = "aeiou  ";

            buff.append(s.charAt(rnd.nextInt(s.length())));
        }
        return buff.toString();
    }

    private static synchronized int getNextCustomerId() {
        return customerCount++;
    }

    private static synchronized int increaseOrders() {
        return orderCount++;
    }

    private static synchronized int increaseOrderLines(int count) {
        return orderLineCount += count;
    }

    private static int getCustomerCount() {
        return customerCount;
    }

    @Override
    void first() throws SQLException {
        Connection c = base.getConnection();
        c.createStatement().execute("drop table customer if exists");
        c.createStatement().execute("drop table orders if exists");
        c.createStatement().execute("drop table orderLine if exists");
        c.createStatement().execute("create table customer(" +
                "id int primary key, name varchar, account decimal)");
        c.createStatement().execute("create table orders(" +
                "id int identity primary key, customer_id int, total decimal)");
        c.createStatement().execute("create table orderLine(" +
                "order_id int, line_id int, text varchar, " +
                "amount decimal, primary key(order_id, line_id))");
        c.close();
    }

    @Override
    void finalTest() throws SQLException {
        conn = base.getConnection();
        ResultSet rs = conn.createStatement().executeQuery(
                "select count(*) from customer");
        rs.next();
        base.assertEquals(customerCount, rs.getInt(1));
        // System.out.println("customers: " + rs.getInt(1));

        rs = conn.createStatement().executeQuery(
                "select count(*) from orders");
        rs.next();
        base.assertEquals(orderCount, rs.getInt(1));
        // System.out.println("orders: " + rs.getInt(1));

        rs = conn.createStatement().executeQuery(
                "select count(*) from orderLine");
        rs.next();
        base.assertEquals(orderLineCount, rs.getInt(1));
        // System.out.println("orderLines: " + rs.getInt(1));

        conn.close();
    }
}
