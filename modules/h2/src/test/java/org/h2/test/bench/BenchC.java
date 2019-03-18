/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * This test is similar to the TPC-C test of the Transaction Processing Council
 * (TPC). Only one connection and one thread is used. Referential integrity is
 * not implemented.
 * <p>
 * See also http://www.tpc.org
 */
public class BenchC implements Bench {

    private static final int COMMIT_EVERY = 1000;

    private static final String[] TABLES = { "WAREHOUSE", "DISTRICT",
            "CUSTOMER", "HISTORY", "ORDERS", "NEW_ORDER", "ITEM", "STOCK",
            "ORDER_LINE", "RESULTS" };

    private static final String[] CREATE_SQL = {
            "CREATE TABLE  WAREHOUSE(\n" +
            " W_ID INT NOT NULL PRIMARY KEY,\n" +
            " W_NAME VARCHAR(10),\n" +
            " W_STREET_1 VARCHAR(20),\n" +
            " W_STREET_2 VARCHAR(20),\n" +
            " W_CITY VARCHAR(20),\n" +
            " W_STATE CHAR(2),\n" +
            " W_ZIP CHAR(9),\n" +
            " W_TAX DECIMAL(4, 4),\n" +
            " W_YTD DECIMAL(12, 2))",
            "CREATE TABLE  DISTRICT(\n" +
            " D_ID INT NOT NULL,\n" +
            " D_W_ID INT NOT NULL,\n" +
            " D_NAME VARCHAR(10),\n" +
            " D_STREET_1 VARCHAR(20),\n" +
            " D_STREET_2 VARCHAR(20),\n" +
            " D_CITY VARCHAR(20),\n" +
            " D_STATE CHAR(2),\n" +
            " D_ZIP CHAR(9),\n" +
            " D_TAX DECIMAL(4, 4),\n" +
            " D_YTD DECIMAL(12, 2),\n" +
            " D_NEXT_O_ID INT,\n" +
            " PRIMARY KEY (D_ID, D_W_ID))",
            // + " FOREIGN KEY (D_W_ID)\n"
            // + " REFERENCES WAREHOUSE(W_ID))",
            "CREATE TABLE  CUSTOMER(\n" +
            " C_ID INT NOT NULL,\n" +
            " C_D_ID INT NOT NULL,\n" +
            " C_W_ID INT NOT NULL,\n" +
            " C_FIRST VARCHAR(16),\n" +
            " C_MIDDLE CHAR(2),\n" +
            " C_LAST VARCHAR(16),\n" +
            " C_STREET_1 VARCHAR(20),\n" +
            " C_STREET_2 VARCHAR(20),\n" +
            " C_CITY VARCHAR(20),\n" +
            " C_STATE CHAR(2),\n" +
            " C_ZIP CHAR(9),\n" +
            " C_PHONE CHAR(16),\n" +
            " C_SINCE TIMESTAMP,\n" +
            " C_CREDIT CHAR(2),\n" +
            " C_CREDIT_LIM DECIMAL(12, 2),\n" +
            " C_DISCOUNT DECIMAL(4, 4),\n" +
            " C_BALANCE DECIMAL(12, 2),\n" +
            " C_YTD_PAYMENT DECIMAL(12, 2),\n" +
            " C_PAYMENT_CNT DECIMAL(4),\n" +
            " C_DELIVERY_CNT DECIMAL(4),\n" +
            " C_DATA VARCHAR(500),\n" +
            " PRIMARY KEY (C_W_ID, C_D_ID, C_ID))",
            // + " FOREIGN KEY (C_W_ID, C_D_ID)\n"
            // + " REFERENCES DISTRICT(D_W_ID, D_ID))",
            "CREATE INDEX CUSTOMER_NAME ON CUSTOMER(C_LAST, C_D_ID, C_W_ID)",
            "CREATE TABLE  HISTORY(\n" +
            " H_C_ID INT,\n" +
            " H_C_D_ID INT,\n" +
            " H_C_W_ID INT,\n" +
            " H_D_ID INT,\n" +
            " H_W_ID INT,\n" +
            " H_DATE TIMESTAMP,\n" +
            " H_AMOUNT DECIMAL(6, 2),\n" +
            " H_DATA VARCHAR(24))",
            // + " FOREIGN KEY(H_C_W_ID, H_C_D_ID, H_C_ID)\n"
            // + " REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID),\n"
            // + " FOREIGN KEY(H_W_ID, H_D_ID)\n"
            // + " REFERENCES DISTRICT(D_W_ID, D_ID))",
            "CREATE TABLE  ORDERS(\n" +
            " O_ID INT NOT NULL,\n" +
            " O_D_ID INT NOT NULL,\n" +
            " O_W_ID INT NOT NULL,\n" +
            " O_C_ID INT,\n" +
            " O_ENTRY_D TIMESTAMP,\n" +
            " O_CARRIER_ID INT,\n" +
            " O_OL_CNT INT,\n" +
            " O_ALL_LOCAL DECIMAL(1),\n" +
            " PRIMARY KEY(O_W_ID, O_D_ID, O_ID))",
            // + " FOREIGN KEY(O_W_ID, O_D_ID, O_C_ID)\n"
            // + " REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID))",
            "CREATE INDEX ORDERS_OID ON ORDERS(O_ID)",
            "CREATE TABLE  NEW_ORDER(\n" +
            " NO_O_ID INT NOT NULL,\n" +
            " NO_D_ID INT NOT NULL,\n" +
            " NO_W_ID INT NOT NULL,\n" +
            " PRIMARY KEY(NO_W_ID, NO_D_ID, NO_O_ID))",
            // + " FOREIGN KEY(NO_W_ID, NO_D_ID, NO_O_ID)\n"
            // + " REFERENCES ORDER(O_W_ID, O_D_ID, O_ID))",
            "CREATE TABLE  ITEM(\n" +
            " I_ID INT NOT NULL,\n" +
            " I_IM_ID INT,\n" +
            " I_NAME VARCHAR(24),\n" +
            " I_PRICE DECIMAL(5, 2),\n" +
            " I_DATA VARCHAR(50),\n" +
            " PRIMARY KEY(I_ID))",
            "CREATE TABLE  STOCK(\n" +
            " S_I_ID INT NOT NULL,\n" +
            " S_W_ID INT NOT NULL,\n" +
            " S_QUANTITY DECIMAL(4),\n" +
            " S_DIST_01 CHAR(24),\n" +
            " S_DIST_02 CHAR(24),\n" +
            " S_DIST_03 CHAR(24),\n" +
            " S_DIST_04 CHAR(24),\n" +
            " S_DIST_05 CHAR(24),\n" +
            " S_DIST_06 CHAR(24),\n" +
            " S_DIST_07 CHAR(24),\n" +
            " S_DIST_08 CHAR(24),\n" +
            " S_DIST_09 CHAR(24),\n" +
            " S_DIST_10 CHAR(24),\n" +
            " S_YTD DECIMAL(8),\n" +
            " S_ORDER_CNT DECIMAL(4),\n" +
            " S_REMOTE_CNT DECIMAL(4),\n" +
            " S_DATA VARCHAR(50),\n" +
            " PRIMARY KEY(S_W_ID, S_I_ID))",
            // + " FOREIGN KEY(S_W_ID)\n"
            // + " REFERENCES WAREHOUSE(W_ID),\n"
            // + " FOREIGN KEY(S_I_ID)\n" + " REFERENCES ITEM(I_ID))",
            "CREATE TABLE  ORDER_LINE(\n" +
            " OL_O_ID INT NOT NULL,\n" +
            " OL_D_ID INT NOT NULL,\n" +
            " OL_W_ID INT NOT NULL,\n" +
            " OL_NUMBER INT NOT NULL,\n" +
            " OL_I_ID INT,\n" +
            " OL_SUPPLY_W_ID INT,\n" +
            " OL_DELIVERY_D TIMESTAMP,\n" +
            " OL_QUANTITY DECIMAL(2),\n" +
            " OL_AMOUNT DECIMAL(6, 2),\n" +
            " OL_DIST_INFO CHAR(24),\n" +
            " PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER))",
            // + " FOREIGN KEY(OL_W_ID, OL_D_ID, OL_O_ID)\n"
            // + " REFERENCES ORDER(O_W_ID, O_D_ID, O_ID),\n"
            // + " FOREIGN KEY(OL_SUPPLY_W_ID, OL_I_ID)\n"
            // + " REFERENCES STOCK(S_W_ID, S_I_ID))",
            "CREATE TABLE RESULTS(\n" +
            " ID INT NOT NULL PRIMARY KEY,\n" +
            " TERMINAL INT,\n" +
            " OPERATION INT,\n" +
            " RESPONSE_TIME INT,\n" +
            " PROCESSING_TIME INT,\n" +
            " KEYING_TIME INT,\n" +
            " THINK_TIME INT,\n" +
            " SUCCESSFUL INT,\n" +
            " NOW TIMESTAMP)" };

    int warehouses = 2;
    int items = 10000;
    int districtsPerWarehouse = 10;
    int customersPerDistrict = 300;

    private Database database;

    private int ordersPerDistrict = 300;

    private BenchCRandom random;
    private String action;

    @Override
    public void init(Database db, int size) throws SQLException {
        this.database = db;

        random = new BenchCRandom();

        items = size * 10;
        warehouses = 2;
        districtsPerWarehouse = Math.max(1, size / 100);
        customersPerDistrict = Math.max(1, size / 100);
        ordersPerDistrict = Math.max(1, size / 1000);

        db.start(this, "Init");
        db.openConnection();
        load();
        db.commit();
        db.closeConnection();
        db.end();

        // db.start(this, "Open/Close");
        // db.openConnection();
        // db.closeConnection();
        // db.end();

    }

    private void load() throws SQLException {
        for (String sql : TABLES) {
            database.dropTable(sql);
        }
        for (String sql : CREATE_SQL) {
            database.update(sql);
        }
        database.setAutoCommit(false);
        loadItem();
        loadWarehouse();
        loadCustomer();
        loadOrder();
        database.commit();
        trace("Load done");
    }

    private void trace(String s) {
        action = s;
    }

    private void trace(int i, int max) {
        database.trace(action, i, max);
    }

    private void loadItem() throws SQLException {
        trace("Loading item table");
        boolean[] original = random.getBoolean(items, items / 10);
        PreparedStatement prep = database.prepare(
                "INSERT INTO ITEM(I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA) " +
                "VALUES(?, ?, ?, ?, ?)");
        for (int id = 1; id <= items; id++) {
            String name = random.getString(14, 24);
            BigDecimal price = random.getBigDecimal(random.getInt(100, 10000), 2);
            String data = random.getString(26, 50);
            if (original[id - 1]) {
                data = random.replace(data, "original");
            }
            prep.setInt(1, id);
            prep.setInt(2, random.getInt(1, 10000));
            prep.setString(3, name);
            prep.setBigDecimal(4, price);
            prep.setString(5, data);
            database.update(prep, "insertItem");
            trace(id, items);
            if (id % COMMIT_EVERY == 0) {
                database.commit();
            }
        }
    }

    private void loadWarehouse() throws SQLException {
        trace("Loading warehouse table");
        PreparedStatement prep = database.prepare(
                "INSERT INTO WAREHOUSE(W_ID, W_NAME, W_STREET_1, " +
                "W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
        for (int id = 1; id <= warehouses; id++) {
            String name = random.getString(6, 10);
            String[] address = random.getAddress();
            String street1 = address[0];
            String street2 = address[1];
            String city = address[2];
            String state = address[3];
            String zip = address[4];
            BigDecimal tax = random.getBigDecimal(random.getInt(0, 2000), 4);
            BigDecimal ytd = new BigDecimal("300000.00");
            prep.setInt(1, id);
            prep.setString(2, name);
            prep.setString(3, street1);
            prep.setString(4, street2);
            prep.setString(5, city);
            prep.setString(6, state);
            prep.setString(7, zip);
            prep.setBigDecimal(8, tax);
            prep.setBigDecimal(9, ytd);
            database.update(prep, "insertWarehouse");
            loadStock(id);
            loadDistrict(id);
            if (id % COMMIT_EVERY == 0) {
                database.commit();
            }
        }
    }

    private void loadCustomer() throws SQLException {
        trace("Load customer table");
        int max = warehouses * districtsPerWarehouse;
        int i = 0;
        for (int id = 1; id <= warehouses; id++) {
            for (int districtId = 1; districtId <= districtsPerWarehouse; districtId++) {
                loadCustomerSub(districtId, id);
                trace(i++, max);
                if (i % COMMIT_EVERY == 0) {
                    database.commit();
                }
            }
        }
    }

    private void loadCustomerSub(int dId, int wId) throws SQLException {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        PreparedStatement prepCustomer = database.prepare(
                "INSERT INTO CUSTOMER(C_ID, C_D_ID, C_W_ID, " +
                "C_FIRST, C_MIDDLE, C_LAST, " +
                "C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, " +
                "C_PHONE, C_SINCE, C_CREDIT, " +
                "C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA, " +
                "C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement prepHistory = database.prepare(
                "INSERT INTO HISTORY(H_C_ID, H_C_D_ID, H_C_W_ID, " +
                "H_W_ID, H_D_ID, H_DATE, H_AMOUNT, H_DATA) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        for (int cId = 1; cId <= customersPerDistrict; cId++) {
            String first = random.getString(8, 16);
            String middle = "OE";
            String last;
            if (cId < 1000) {
                last = random.getLastname(cId);
            } else {
                last = random.getLastname(random.getNonUniform(255, 0, 999));
            }
            String[] address = random.getAddress();
            String street1 = address[0];
            String street2 = address[1];
            String city = address[2];
            String state = address[3];
            String zip = address[4];
            String phone = random.getNumberString(16, 16);
            String credit;
            if (random.getInt(0, 1) == 0) {
                credit = "GC";
            } else {
                credit = "BC";
            }
            BigDecimal discount = random.getBigDecimal(random.getInt(0, 5000), 4);
            BigDecimal balance = new BigDecimal("-10.00");
            BigDecimal creditLim = new BigDecimal("50000.00");
            String data = random.getString(300, 500);
            BigDecimal ytdPayment = new BigDecimal("10.00");
            int paymentCnt = 1;
            int deliveryCnt = 1;
            prepCustomer.setInt(1, cId);
            prepCustomer.setInt(2, dId);
            prepCustomer.setInt(3, wId);
            prepCustomer.setString(4, first);
            prepCustomer.setString(5, middle);
            prepCustomer.setString(6, last);
            prepCustomer.setString(7, street1);
            prepCustomer.setString(8, street2);
            prepCustomer.setString(9, city);
            prepCustomer.setString(10, state);
            prepCustomer.setString(11, zip);
            prepCustomer.setString(12, phone);
            prepCustomer.setTimestamp(13, timestamp);
            prepCustomer.setString(14, credit);
            prepCustomer.setBigDecimal(15, creditLim);
            prepCustomer.setBigDecimal(16, discount);
            prepCustomer.setBigDecimal(17, balance);
            prepCustomer.setString(18, data);
            prepCustomer.setBigDecimal(19, ytdPayment);
            prepCustomer.setInt(20, paymentCnt);
            prepCustomer.setInt(21, deliveryCnt);
            database.update(prepCustomer, "insertCustomer");
            BigDecimal amount = new BigDecimal("10.00");
            String hData = random.getString(12, 24);
            prepHistory.setInt(1, cId);
            prepHistory.setInt(2, dId);
            prepHistory.setInt(3, wId);
            prepHistory.setInt(4, wId);
            prepHistory.setInt(5, dId);
            prepHistory.setTimestamp(6, timestamp);
            prepHistory.setBigDecimal(7, amount);
            prepHistory.setString(8, hData);
            database.update(prepHistory, "insertHistory");
        }
    }

    private void loadOrder() throws SQLException {
        trace("Loading order table");
        int max = warehouses * districtsPerWarehouse;
        int i = 0;
        for (int wId = 1; wId <= warehouses; wId++) {
            for (int dId = 1; dId <= districtsPerWarehouse; dId++) {
                loadOrderSub(dId, wId);
                trace(i++, max);
            }
        }
    }

    private void loadOrderSub(int dId, int wId) throws SQLException {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        int[] orderid = random.getPermutation(ordersPerDistrict);
        PreparedStatement prepOrder = database.prepare(
                "INSERT INTO ORDERS(O_ID, O_C_ID, O_D_ID, O_W_ID, " +
                "O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, 1)");
        PreparedStatement prepNewOrder = database.prepare(
                "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) " +
                "VALUES (?, ?, ?)");
        PreparedStatement prepLine = database.prepare(
                "INSERT INTO ORDER_LINE(" +
                "OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, " +
                "OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, " +
                "OL_DIST_INFO, OL_DELIVERY_D)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)");
        for (int oId = 1, i = 0; oId <= ordersPerDistrict; oId++) {
            int cId = orderid[oId - 1];
            int carrierId = random.getInt(1, 10);
            int olCnt = random.getInt(5, 15);
            prepOrder.setInt(1, oId);
            prepOrder.setInt(2, cId);
            prepOrder.setInt(3, dId);
            prepOrder.setInt(4, wId);
            prepOrder.setTimestamp(5, timestamp);
            prepOrder.setInt(7, olCnt);
            if (oId <= 2100) {
                prepOrder.setInt(6, carrierId);
            } else {
                // the last 900 orders have not been delivered
                prepOrder.setNull(6, Types.INTEGER);
                prepNewOrder.setInt(1, oId);
                prepNewOrder.setInt(2, dId);
                prepNewOrder.setInt(3, wId);
                database.update(prepNewOrder, "newNewOrder");
            }
            database.update(prepOrder, "insertOrder");
            for (int ol = 1; ol <= olCnt; ol++) {
                int id = random.getInt(1, items);
                int supplyId = wId;
                int quantity = 5;
                String distInfo = random.getString(24);
                BigDecimal amount;
                if (oId < 2101) {
                    amount = random.getBigDecimal(0, 2);
                } else {
                    amount = random.getBigDecimal(random.getInt(0, 1000000), 2);
                }
                prepLine.setInt(1, oId);
                prepLine.setInt(2, dId);
                prepLine.setInt(3, wId);
                prepLine.setInt(4, ol);
                prepLine.setInt(5, id);
                prepLine.setInt(6, supplyId);
                prepLine.setInt(7, quantity);
                prepLine.setBigDecimal(8, amount);
                prepLine.setString(9, distInfo);
                database.update(prepLine, "insertOrderLine");
                if (i++ % COMMIT_EVERY == 0) {
                    database.commit();
                }
            }
        }
    }

    private void loadStock(int wId) throws SQLException {
        trace("Loading stock table (warehouse " + wId + ")");
        boolean[] original = random.getBoolean(items, items / 10);
        PreparedStatement prep = database.prepare(
                "INSERT INTO STOCK(S_I_ID, S_W_ID, S_QUANTITY, " +
                "S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
                "S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, " +
                "S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        for (int id = 1; id <= items; id++) {
            int quantity = random.getInt(10, 100);
            String dist01 = random.getString(24);
            String dist02 = random.getString(24);
            String dist03 = random.getString(24);
            String dist04 = random.getString(24);
            String dist05 = random.getString(24);
            String dist06 = random.getString(24);
            String dist07 = random.getString(24);
            String dist08 = random.getString(24);
            String dist09 = random.getString(24);
            String dist10 = random.getString(24);
            String data = random.getString(26, 50);
            if (original[id - 1]) {
                data = random.replace(data, "original");
            }
            prep.setInt(1, id);
            prep.setInt(2, wId);
            prep.setInt(3, quantity);
            prep.setString(4, dist01);
            prep.setString(5, dist02);
            prep.setString(6, dist03);
            prep.setString(7, dist04);
            prep.setString(8, dist05);
            prep.setString(9, dist06);
            prep.setString(10, dist07);
            prep.setString(11, dist08);
            prep.setString(12, dist09);
            prep.setString(13, dist10);
            prep.setString(14, data);
            prep.setInt(15, 0);
            prep.setInt(16, 0);
            prep.setInt(17, 0);
            database.update(prep, "insertStock");
            if (id % COMMIT_EVERY == 0) {
                database.commit();
            }
            trace(id, items);
        }
    }

    private void loadDistrict(int wId) throws SQLException {
        BigDecimal ytd = new BigDecimal("300000.00");
        int nextId = 3001;
        PreparedStatement prep = database.prepare(
                "INSERT INTO DISTRICT(D_ID, D_W_ID, D_NAME, " +
                "D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, " +
                "D_TAX, D_YTD, D_NEXT_O_ID) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        for (int dId = 1; dId <= districtsPerWarehouse; dId++) {
            String name = random.getString(6, 10);
            String[] address = random.getAddress();
            String street1 = address[0];
            String street2 = address[1];
            String city = address[2];
            String state = address[3];
            String zip = address[4];
            BigDecimal tax = random.getBigDecimal(random.getInt(0, 2000), 4);
            prep.setInt(1, dId);
            prep.setInt(2, wId);
            prep.setString(3, name);
            prep.setString(4, street1);
            prep.setString(5, street2);
            prep.setString(6, city);
            prep.setString(7, state);
            prep.setString(8, zip);
            prep.setBigDecimal(9, tax);
            prep.setBigDecimal(10, ytd);
            prep.setInt(11, nextId);
            database.update(prep, "insertDistrict");
            trace(dId, districtsPerWarehouse);
        }
    }

    @Override
    public void runTest() throws SQLException {
        database.start(this, "Transactions");
        database.openConnection();
        for (int i = 0; i < 70; i++) {
            BenchCThread process = new BenchCThread(database, this, random, i);
            process.process();
        }
        database.closeConnection();
        database.end();

        database.openConnection();
        BenchCThread process = new BenchCThread(database, this, random, 0);
        process.process();
        database.logMemory(this, "Memory Usage");
        database.closeConnection();
    }

    @Override
    public String getName() {
        return "BenchC";
    }

}
