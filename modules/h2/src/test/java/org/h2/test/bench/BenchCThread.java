/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;

/**
 * This class implements the functionality of one thread of BenchC.
 */
public class BenchCThread {

    private static final int OP_NEW_ORDER = 0, OP_PAYMENT = 1,
            OP_ORDER_STATUS = 2, OP_DELIVERY = 3,
            OP_STOCK_LEVEL = 4;
    private static final BigDecimal ONE = new BigDecimal("1");

    private final Database db;
    private final int warehouseId;
    private final int terminalId;
    private final HashMap<String, PreparedStatement> prepared =
            new HashMap<>();
    private final BenchCRandom random;
    private final BenchC bench;

    BenchCThread(Database db, BenchC bench, BenchCRandom random, int terminal)
            throws SQLException {
        this.db = db;
        this.bench = bench;
        this.terminalId = terminal;
        db.setAutoCommit(false);
        this.random = random;
        warehouseId = random.getInt(1, bench.warehouses);
    }

    /**
     * Process the list of operations (a 'deck') in random order.
     */
    void process() throws SQLException {
        int[] deck = { OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER,
                OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER,
                OP_NEW_ORDER, OP_NEW_ORDER, OP_NEW_ORDER, OP_PAYMENT,
                OP_PAYMENT, OP_PAYMENT, OP_PAYMENT, OP_PAYMENT, OP_PAYMENT,
                OP_PAYMENT, OP_PAYMENT, OP_PAYMENT, OP_PAYMENT,
                OP_ORDER_STATUS, OP_DELIVERY, OP_STOCK_LEVEL };
        int len = deck.length;
        for (int i = 0; i < len; i++) {
            int temp = deck[i];
            int j = random.getInt(0, len);
            deck[i] = deck[j];
            deck[j] = temp;
        }
        for (int op : deck) {
            switch (op) {
            case OP_NEW_ORDER:
                processNewOrder();
                break;
            case OP_PAYMENT:
                processPayment();
                break;
            case OP_ORDER_STATUS:
                processOrderStatus();
                break;
            case OP_DELIVERY:
                processDelivery();
                break;
            case OP_STOCK_LEVEL:
                processStockLevel();
                break;
            default:
                throw new AssertionError("op=" + op);
            }
        }
    }

    private void processNewOrder() throws SQLException {
        int dId = random.getInt(1, bench.districtsPerWarehouse);
        int cId = random.getNonUniform(1023, 1, bench.customersPerDistrict);
        int olCnt = random.getInt(5, 15);
        boolean rollback = random.getInt(1, 100) == 1;
        int[] supplyId = new int[olCnt];
        int[] itemId = new int[olCnt];
        int[] quantity = new int[olCnt];
        int allLocal = 1;
        for (int i = 0; i < olCnt; i++) {
            int w;
            if (bench.warehouses > 1 && random.getInt(1, 100) == 1) {
                do {
                    w = random.getInt(1, bench.warehouses);
                } while (w != warehouseId);
                allLocal = 0;
            } else {
                w = warehouseId;
            }
            supplyId[i] = w;
            int item;
            if (rollback && i == olCnt - 1) {
                // unused order number
                item = -1;
            } else {
                item = random.getNonUniform(8191, 1, bench.items);
            }
            itemId[i] = item;
            quantity[i] = random.getInt(1, 10);
        }
        char[] bg = new char[olCnt];
        int[] stock = new int[olCnt];
        BigDecimal[] amt = new BigDecimal[olCnt];
        Timestamp datetime = new Timestamp(System.currentTimeMillis());
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=D_NEXT_O_ID+1 "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, dId);
        prep.setInt(2, warehouseId);
        db.update(prep, "updateDistrict");
        prep = prepare("SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, dId);
        prep.setInt(2, warehouseId);
        rs = db.query(prep);
        rs.next();
        int oId = rs.getInt(1) - 1;
        BigDecimal tax = rs.getBigDecimal(2);
        rs.close();
        prep = prepare("SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX "
                + "FROM CUSTOMER, WAREHOUSE "
                + "WHERE C_ID=? AND W_ID=? AND C_W_ID=W_ID AND C_D_ID=?");
        prep.setInt(1, cId);
        prep.setInt(2, warehouseId);
        prep.setInt(3, dId);
        rs = db.query(prep);
        rs.next();
        BigDecimal discount = rs.getBigDecimal(1);
        // c_last
        rs.getString(2);
        // c_credit
        rs.getString(3);
        BigDecimal wTax = rs.getBigDecimal(4);
        rs.close();
        BigDecimal total = new BigDecimal("0");
        for (int number = 1; number <= olCnt; number++) {
            int olId = itemId[number - 1];
            int olSupplyId = supplyId[number - 1];
            int olQuantity = quantity[number - 1];
            prep = prepare("SELECT I_PRICE, I_NAME, I_DATA "
                    + "FROM ITEM WHERE I_ID=?");
            prep.setInt(1, olId);
            rs = db.query(prep);
            if (!rs.next()) {
                if (rollback) {
                    // item not found - correct behavior
                    db.rollback();
                    return;
                }
                throw new SQLException("item not found: " + olId + " "
                        + olSupplyId);
            }
            BigDecimal price = rs.getBigDecimal(1);
            // i_name
            rs.getString(2);
            String data = rs.getString(3);
            rs.close();
            prep = prepare("SELECT S_QUANTITY, S_DATA, "
                    + "S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
                    + "S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 "
                    + "FROM STOCK WHERE S_I_ID=? AND S_W_ID=?");
            prep.setInt(1, olId);
            prep.setInt(2, olSupplyId);
            rs = db.query(prep);
            if (!rs.next()) {
                if (rollback) {
                    // item not found - correct behavior
                    db.rollback();
                    return;
                }
                throw new SQLException("item not found: " + olId + " "
                        + olSupplyId);
            }
            int sQuantity = rs.getInt(1);
            String sData = rs.getString(2);
            String[] dist = new String[10];
            for (int i = 0; i < 10; i++) {
                dist[i] = rs.getString(3 + i);
            }
            rs.close();
            String distInfo = dist[(dId - 1) % 10];
            stock[number - 1] = sQuantity;
            if (data.contains("original")
                    && sData.contains("original")) {
                bg[number - 1] = 'B';
            } else {
                bg[number - 1] = 'G';
            }
            if (sQuantity > olQuantity) {
                sQuantity = sQuantity - olQuantity;
            } else {
                sQuantity = sQuantity - olQuantity + 91;
            }
            prep = prepare("UPDATE STOCK SET S_QUANTITY=? "
                    + "WHERE S_W_ID=? AND S_I_ID=?");
            prep.setInt(1, sQuantity);
            prep.setInt(2, olSupplyId);
            prep.setInt(3, olId);
            db.update(prep, "updateStock");
            BigDecimal olAmount = new BigDecimal(olQuantity).multiply(
                    price).multiply(ONE.add(wTax).add(tax)).multiply(
                    ONE.subtract(discount));
            olAmount = olAmount.setScale(2, BigDecimal.ROUND_HALF_UP);
            amt[number - 1] = olAmount;
            total = total.add(olAmount);
            prep = prepare("INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, "
                    + "OL_I_ID, OL_SUPPLY_W_ID, "
                    + "OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
            prep.setInt(1, oId);
            prep.setInt(2, dId);
            prep.setInt(3, warehouseId);
            prep.setInt(4, number);
            prep.setInt(5, olId);
            prep.setInt(6, olSupplyId);
            prep.setInt(7, olQuantity);
            prep.setBigDecimal(8, olAmount);
            prep.setString(9, distInfo);
            db.update(prep, "insertOrderLine");
        }
        prep = prepare("INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, "
                + "O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)");
        prep.setInt(1, oId);
        prep.setInt(2, dId);
        prep.setInt(3, warehouseId);
        prep.setInt(4, cId);
        prep.setTimestamp(5, datetime);
        prep.setInt(6, olCnt);
        prep.setInt(7, allLocal);
        db.update(prep, "insertOrders");
        prep = prepare("INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) "
                + "VALUES (?, ?, ?)");
        prep.setInt(1, oId);
        prep.setInt(2, dId);
        prep.setInt(3, warehouseId);
        db.update(prep, "insertNewOrder");
        db.commit();
    }

    private void processPayment() throws SQLException {
        int dId = random.getInt(1, bench.districtsPerWarehouse);
        int wId, cdId;
        if (bench.warehouses > 1 && random.getInt(1, 100) <= 15) {
            do {
                wId = random.getInt(1, bench.warehouses);
            } while (wId != warehouseId);
            cdId = random.getInt(1, bench.districtsPerWarehouse);
        } else {
            wId = warehouseId;
            cdId = dId;
        }
        boolean byName;
        String last;
        int cId = 1;
        if (random.getInt(1, 100) <= 60) {
            byName = true;
            last = random.getLastname(random.getNonUniform(255, 0, 999));
        } else {
            byName = false;
            last = "";
            cId = random.getNonUniform(1023, 1, bench.customersPerDistrict);
        }
        BigDecimal amount = random.getBigDecimal(random.getInt(100, 500000),
                2);
        Timestamp datetime = new Timestamp(System.currentTimeMillis());
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_YTD = D_YTD+? "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setBigDecimal(1, amount);
        prep.setInt(2, dId);
        prep.setInt(3, warehouseId);
        db.update(prep, "updateDistrict");
        prep = prepare("UPDATE WAREHOUSE SET W_YTD=W_YTD+? WHERE W_ID=?");
        prep.setBigDecimal(1, amount);
        prep.setInt(2, warehouseId);
        db.update(prep, "updateWarehouse");
        prep = prepare("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME "
                + "FROM WAREHOUSE WHERE W_ID=?");
        prep.setInt(1, warehouseId);
        rs = db.query(prep);
        rs.next();
        // w_street_1
        rs.getString(1);
        // w_street_2
        rs.getString(2);
        // w_city
        rs.getString(3);
        // w_state
        rs.getString(4);
        // w_zip
        rs.getString(5);
        String wName = rs.getString(6);
        rs.close();
        prep = prepare("SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME "
                + "FROM DISTRICT WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, dId);
        prep.setInt(2, warehouseId);
        rs = db.query(prep);
        rs.next();
        // d_street_1
        rs.getString(1);
        // d_street_2
        rs.getString(2);
        // d_city
        rs.getString(3);
        // d_state
        rs.getString(4);
        // d_zip
        rs.getString(5);
        String dName = rs.getString(6);
        rs.close();
        BigDecimal balance;
        String credit;
        if (byName) {
            prep = prepare("SELECT COUNT(C_ID) FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=?");
            prep.setString(1, last);
            prep.setInt(2, cdId);
            prep.setInt(3, wId);
            rs = db.query(prep);
            rs.next();
            int namecnt = rs.getInt(1);
            rs.close();
            if (namecnt == 0) {
                // TODO TPC-C: check if this can happen
                db.rollback();
                return;
            }
            prep = prepare("SELECT C_FIRST, C_MIDDLE, C_ID, "
                    + "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, "
                    + "C_PHONE, C_CREDIT, C_CREDIT_LIM, "
                    + "C_DISCOUNT, C_BALANCE, C_SINCE FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=? "
                    + "ORDER BY C_FIRST");
            prep.setString(1, last);
            prep.setInt(2, cdId);
            prep.setInt(3, wId);
            rs = db.query(prep);
            // locate midpoint customer
            if (namecnt % 2 != 0) {
                namecnt++;
            }
            for (int n = 0; n < namecnt / 2; n++) {
                rs.next();
            }
            // c_first
            rs.getString(1);
            // c_middle
            rs.getString(2);
            cId = rs.getInt(3);
            // c_street_1
            rs.getString(4);
            // c_street_2
            rs.getString(5);
            // c_city
            rs.getString(6);
            // c_state
            rs.getString(7);
            // c_zip
            rs.getString(8);
            // c_phone
            rs.getString(9);
            credit = rs.getString(10);
            // c_credit_lim
            rs.getString(11);
            // c_discount
            rs.getBigDecimal(12);
            balance = rs.getBigDecimal(13);
            // c_since
            rs.getTimestamp(14);
            rs.close();
        } else {
            prep = prepare("SELECT C_FIRST, C_MIDDLE, C_LAST, "
                    + "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, "
                    + "C_PHONE, C_CREDIT, C_CREDIT_LIM, "
                    + "C_DISCOUNT, C_BALANCE, C_SINCE FROM CUSTOMER "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setInt(1, cId);
            prep.setInt(2, cdId);
            prep.setInt(3, wId);
            rs = db.query(prep);
            rs.next();
            // c_first
            rs.getString(1);
            // c_middle
            rs.getString(2);
            // c_last
            rs.getString(3);
            // c_street_1
            rs.getString(4);
            // c_street_2
            rs.getString(5);
            // c_city
            rs.getString(6);
            // c_state
            rs.getString(7);
            // c_zip
            rs.getString(8);
            // c_phone
            rs.getString(9);
            credit = rs.getString(10);
            // c_credit_lim
            rs.getString(11);
            // c_discount
            rs.getBigDecimal(12);
            balance = rs.getBigDecimal(13);
            // c_since
            rs.getTimestamp(14);
            rs.close();
        }
        balance = balance.add(amount);
        if (credit.equals("BC")) {
            prep = prepare("SELECT C_DATA INTO FROM CUSTOMER "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setInt(1, cId);
            prep.setInt(2, cdId);
            prep.setInt(3, wId);
            rs = db.query(prep);
            rs.next();
            String cData = rs.getString(1);
            rs.close();
            String cNewData = "| " + cId + " " + cdId + " " + wId
                    + " " + dId + " " + warehouseId + " " + amount + " "
                    + cData;
            if (cNewData.length() > 500) {
                cNewData = cNewData.substring(0, 500);
            }
            prep = prepare("UPDATE CUSTOMER SET C_BALANCE=?, C_DATA=? "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setBigDecimal(1, balance);
            prep.setString(2, cNewData);
            prep.setInt(3, cId);
            prep.setInt(4, cdId);
            prep.setInt(5, wId);
            db.update(prep, "updateCustomer");
        } else {
            prep = prepare("UPDATE CUSTOMER SET C_BALANCE=? "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setBigDecimal(1, balance);
            prep.setInt(2, cId);
            prep.setInt(3, cdId);
            prep.setInt(4, wId);
            db.update(prep, "updateCustomer");
        }
        // MySQL bug?
//        String h_data = w_name + "    " + d_name;
        String hData = wName + " " + dName;
        prep = prepare("INSERT INTO HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, "
                + "H_W_ID, H_DATE, H_AMOUNT, H_DATA) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        prep.setInt(1, cdId);
        prep.setInt(2, wId);
        prep.setInt(3, cId);
        prep.setInt(4, dId);
        prep.setInt(5, warehouseId);
        prep.setTimestamp(6, datetime);
        prep.setBigDecimal(7, amount);
        prep.setString(8, hData);
        db.update(prep, "insertHistory");
        db.commit();
    }

    private void processOrderStatus() throws SQLException {
        int dId = random.getInt(1, bench.districtsPerWarehouse);
        boolean byName;
        String last = null;
        int cId = -1;
        if (random.getInt(1, 100) <= 60) {
            byName = true;
            last = random.getLastname(random.getNonUniform(255, 0, 999));
        } else {
            byName = false;
            cId = random.getNonUniform(1023, 1, bench.customersPerDistrict);
        }
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=-1 WHERE D_ID=-1");
        db.update(prep, "updateDistrict");
        if (byName) {
            prep = prepare("SELECT COUNT(C_ID) FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=?");
            prep.setString(1, last);
            prep.setInt(2, dId);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            rs.next();
            int namecnt = rs.getInt(1);
            rs.close();
            if (namecnt == 0) {
                // TODO TPC-C: check if this can happen
                db.rollback();
                return;
            }
            prep = prepare("SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_ID "
                    + "FROM CUSTOMER "
                    + "WHERE C_LAST=? AND C_D_ID=? AND C_W_ID=? "
                    + "ORDER BY C_FIRST");
            prep.setString(1, last);
            prep.setInt(2, dId);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            if (namecnt % 2 != 0) {
                namecnt++;
            }
            for (int n = 0; n < namecnt / 2; n++) {
                rs.next();
            }
            // c_balance
            rs.getBigDecimal(1);
            // c_first
            rs.getString(2);
            // c_middle
            rs.getString(3);
            rs.close();
        } else {
            prep = prepare("SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_LAST "
                    + "FROM CUSTOMER "
                    + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
            prep.setInt(1, cId);
            prep.setInt(2, dId);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            rs.next();
            // c_balance
            rs.getBigDecimal(1);
            // c_first
            rs.getString(2);
            // c_middle
            rs.getString(3);
            // c_last
            rs.getString(4);
            rs.close();
        }
        prep = prepare("SELECT MAX(O_ID) "
                + "FROM ORDERS WHERE O_C_ID=? AND O_D_ID=? AND O_W_ID=?");
        prep.setInt(1, cId);
        prep.setInt(2, dId);
        prep.setInt(3, warehouseId);
        rs = db.query(prep);
        int oId = -1;
        if (rs.next()) {
            oId = rs.getInt(1);
            if (rs.wasNull()) {
                oId = -1;
            }
        }
        rs.close();
        if (oId != -1) {
            prep = prepare("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D "
                    + "FROM ORDERS WHERE O_ID=?");
            prep.setInt(1, oId);
            rs = db.query(prep);
            rs.next();
            oId = rs.getInt(1);
            // o_carrier_id
            rs.getInt(2);
            // o_entry_d
            rs.getTimestamp(3);
            rs.close();
            prep = prepare("SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, "
                    + "OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE "
                    + "WHERE OL_O_ID=? AND OL_D_ID=? AND OL_W_ID=?");
            prep.setInt(1, oId);
            prep.setInt(2, dId);
            prep.setInt(3, warehouseId);
            rs = db.query(prep);
            while (rs.next()) {
                // o_i_id
                rs.getInt(1);
                // ol_supply_w_id
                rs.getInt(2);
                // ol_quantity
                rs.getInt(3);
                // ol_amount
                rs.getBigDecimal(4);
                // ol_delivery_d
                rs.getTimestamp(5);
            }
            rs.close();
        }
        db.commit();
    }

    private void processDelivery() throws SQLException {
        int carrierId = random.getInt(1, 10);
        Timestamp datetime = new Timestamp(System.currentTimeMillis());
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=-1 WHERE D_ID=-1");
        db.update(prep, "updateDistrict");
        for (int dId = 1; dId <= bench.districtsPerWarehouse; dId++) {
            prep = prepare("SELECT MIN(NO_O_ID) FROM NEW_ORDER "
                    + "WHERE NO_D_ID=? AND NO_W_ID=?");
            prep.setInt(1, dId);
            prep.setInt(2, warehouseId);
            rs = db.query(prep);
            int noId = -1;
            if (rs.next()) {
                noId = rs.getInt(1);
                if (rs.wasNull()) {
                    noId = -1;
                }
            }
            rs.close();
            if (noId != -1) {
                prep = prepare("DELETE FROM NEW_ORDER "
                        + "WHERE NO_O_ID=? AND NO_D_ID=? AND NO_W_ID=?");
                prep.setInt(1, noId);
                prep.setInt(2, dId);
                prep.setInt(3, warehouseId);
                db.update(prep, "deleteNewOrder");
                prep = prepare("SELECT O_C_ID FROM ORDERS "
                        + "WHERE O_ID=? AND O_D_ID=? AND O_W_ID=?");
                prep.setInt(1, noId);
                prep.setInt(2, dId);
                prep.setInt(3, warehouseId);
                rs = db.query(prep);
                rs.next();
                // o_c_id
                rs.getInt(1);
                rs.close();
                prep = prepare("UPDATE ORDERS SET O_CARRIER_ID=? "
                        + "WHERE O_ID=? AND O_D_ID=? AND O_W_ID=?");
                prep.setInt(1, carrierId);
                prep.setInt(2, noId);
                prep.setInt(3, dId);
                prep.setInt(4, warehouseId);
                db.update(prep, "updateOrders");
                prep = prepare("UPDATE ORDER_LINE SET OL_DELIVERY_D=? "
                        + "WHERE OL_O_ID=? AND OL_D_ID=? AND OL_W_ID=?");
                prep.setTimestamp(1, datetime);
                prep.setInt(2, noId);
                prep.setInt(3, dId);
                prep.setInt(4, warehouseId);
                db.update(prep, "updateOrderLine");
                prep = prepare("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE "
                        + "WHERE OL_O_ID=? AND OL_D_ID=? AND OL_W_ID=?");
                prep.setInt(1, noId);
                prep.setInt(2, dId);
                prep.setInt(3, warehouseId);
                rs = db.query(prep);
                rs.next();
                BigDecimal amount = rs.getBigDecimal(1);
                rs.close();
                prep = prepare("UPDATE CUSTOMER SET C_BALANCE=C_BALANCE+? "
                        + "WHERE C_ID=? AND C_D_ID=? AND C_W_ID=?");
                prep.setBigDecimal(1, amount);
                prep.setInt(2, noId);
                prep.setInt(3, dId);
                prep.setInt(4, warehouseId);
                db.update(prep, "updateCustomer");
            }
        }
        db.commit();
    }

    private void processStockLevel() throws SQLException {
        int dId = (terminalId % bench.districtsPerWarehouse) + 1;
        int threshold = random.getInt(10, 20);
        PreparedStatement prep;
        ResultSet rs;

        prep = prepare("UPDATE DISTRICT SET D_NEXT_O_ID=-1 WHERE D_ID=-1");
        db.update(prep, "updateDistrict");

        prep = prepare("SELECT D_NEXT_O_ID FROM DISTRICT "
                + "WHERE D_ID=? AND D_W_ID=?");
        prep.setInt(1, dId);
        prep.setInt(2, warehouseId);
        rs = db.query(prep);
        rs.next();
        int oId = rs.getInt(1);
        rs.close();
        prep = prepare("SELECT COUNT(DISTINCT S_I_ID) "
                + "FROM ORDER_LINE, STOCK WHERE "
                + "OL_W_ID=? AND "
                + "OL_D_ID=? AND "
                + "OL_O_ID<? AND "
                + "OL_O_ID>=?-20 AND "
                + "S_W_ID=? AND "
                + "S_I_ID=OL_I_ID AND "
                + "S_QUANTITY<?");
        prep.setInt(1, warehouseId);
        prep.setInt(2, dId);
        prep.setInt(3, oId);
        prep.setInt(4, oId);
        prep.setInt(5, warehouseId);
        prep.setInt(6, threshold);
        rs = db.query(prep);
        rs.next();
        // stockCount
        rs.getInt(1);
        rs.close();
        db.commit();
    }

    private PreparedStatement prepare(String sql) throws SQLException {
        PreparedStatement prep = prepared.get(sql);
        if (prep == null) {
            prep = db.prepare(sql);
            prepared.put(sql, prep);
        }
        return prep;
    }

}
