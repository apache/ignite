/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;

/**
 * This test is similar to the TPC-A test of the Transaction Processing Council
 * (TPC). However, only one connection and one thread is used.
 * <p>
 * See also: http://www.tpc.org/tpca/spec/tpca_current.pdf
 */
public class BenchA implements Bench {

    private static final String FILLER = "abcdefghijklmnopqrstuvwxyz";
    private static final int DELTA = 10000;

    private Database database;

    private int branches;
    private int tellers;
    private int accounts;
    private int transactions;

    @Override
    public void init(Database db, int size) throws SQLException {
        this.database = db;
        transactions = size * 6;

        int scale = 2;
        accounts = size * 30;
        tellers = Math.max(accounts / 10, 1);
        branches = Math.max(tellers / 10, 1);

        db.start(this, "Init");

        db.openConnection();

        db.dropTable("BRANCHES");
        db.dropTable("TELLERS");
        db.dropTable("ACCOUNTS");
        db.dropTable("HISTORY");

        String[] create = {
                "CREATE TABLE BRANCHES(BID INT NOT NULL PRIMARY KEY, " +
                "BBALANCE DECIMAL(15,2), FILLER VARCHAR(88))",
                "CREATE TABLE TELLERS(TID INT NOT NULL PRIMARY KEY, " +
                "BID INT, TBALANCE DECIMAL(15,2), FILLER VARCHAR(84))",
                "CREATE TABLE ACCOUNTS(AID INT NOT NULL PRIMARY KEY, " +
                "BID INT, ABALANCE DECIMAL(15,2), FILLER VARCHAR(84))",
                "CREATE TABLE HISTORY(TID INT, " +
                "BID INT, AID INT, DELTA DECIMAL(15,2), HTIME DATETIME, " +
                "FILLER VARCHAR(40))" };

        for (String sql : create) {
            db.update(sql);
        }

        PreparedStatement prep;
        db.setAutoCommit(false);
        int commitEvery = 1000;
        prep = db.prepare(
                "INSERT INTO BRANCHES(BID, BBALANCE, FILLER) " +
                "VALUES(?, 10000.00, '" + FILLER + "')");
        for (int i = 0; i < branches * scale; i++) {
            prep.setInt(1, i);
            db.update(prep, "insertBranches");
            if (i % commitEvery == 0) {
                db.commit();
            }
        }
        db.commit();
        prep = db.prepare(
                "INSERT INTO TELLERS(TID, BID, TBALANCE, FILLER) " +
                "VALUES(?, ?, 10000.00, '" + FILLER + "')");
        for (int i = 0; i < tellers * scale; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i / tellers);
            db.update(prep, "insertTellers");
            if (i % commitEvery == 0) {
                db.commit();
            }
        }
        db.commit();
        int len = accounts * scale;
        prep = db.prepare(
                "INSERT INTO ACCOUNTS(AID, BID, ABALANCE, FILLER) " +
                "VALUES(?, ?, 10000.00, '" + FILLER + "')");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i / accounts);
            db.update(prep, "insertAccounts");
            if (i % commitEvery == 0) {
                db.commit();
            }
        }
        db.commit();
        db.closeConnection();
        db.end();

//        db.start(this, "Open/Close");
//        db.openConnection();
//        db.closeConnection();
//        db.end();
    }

    @Override
    public void runTest() throws SQLException {

        database.start(this, "Transactions");
        database.openConnection();
        processTransactions();
        database.closeConnection();
        database.end();

        database.openConnection();
        processTransactions();
        database.logMemory(this, "Memory Usage");
        database.closeConnection();

    }

    private void processTransactions() throws SQLException {
        Random random = database.getRandom();
        int branch = random.nextInt(branches);
        int teller = random.nextInt(tellers);

        PreparedStatement updateAccount = database.prepare(
                "UPDATE ACCOUNTS SET ABALANCE=ABALANCE+? WHERE AID=?");
        PreparedStatement selectBalance = database.prepare(
                "SELECT ABALANCE FROM ACCOUNTS WHERE AID=?");
        PreparedStatement updateTeller = database.prepare(
                "UPDATE TELLERS SET TBALANCE=TBALANCE+? WHERE TID=?");
        PreparedStatement updateBranch = database.prepare(
                "UPDATE BRANCHES SET BBALANCE=BBALANCE+? WHERE BID=?");
        PreparedStatement insertHistory = database.prepare(
                "INSERT INTO HISTORY(AID, TID, BID, DELTA, HTIME, FILLER) " +
                "VALUES(?, ?, ?, ?, ?, ?)");
        int accountsPerBranch = accounts / branches;
        database.setAutoCommit(false);

        for (int i = 0; i < transactions; i++) {
            int account;
            if (random.nextInt(100) < 85) {
                account = random.nextInt(accountsPerBranch) + branch * accountsPerBranch;
            } else {
                account = random.nextInt(accounts);
            }
            int max = BenchA.DELTA;
            // delta: -max .. +max

            BigDecimal delta = BigDecimal.valueOf(random.nextInt(max * 2) - max);
            long current = System.currentTimeMillis();

            updateAccount.setBigDecimal(1, delta);
            updateAccount.setInt(2, account);
            database.update(updateAccount, "updateAccount");

            updateTeller.setBigDecimal(1, delta);
            updateTeller.setInt(2, teller);
            database.update(updateTeller, "updateTeller");

            updateBranch.setBigDecimal(1, delta);
            updateBranch.setInt(2, branch);
            database.update(updateBranch, "updateBranch");

            selectBalance.setInt(1, account);
            database.queryReadResult(selectBalance);

            insertHistory.setInt(1, account);
            insertHistory.setInt(2, teller);
            insertHistory.setInt(3, branch);
            insertHistory.setBigDecimal(4, delta);
            // TODO convert: should be able to convert date to timestamp
            // (by using 0 for remaining fields)
            // insertHistory.setDate(5, new java.sql.Date(current));
            insertHistory.setTimestamp(5, new java.sql.Timestamp(current));
            insertHistory.setString(6, BenchA.FILLER);
            database.update(insertHistory, "insertHistory");

            database.commit();
        }
        updateAccount.close();
        selectBalance.close();
        updateTeller.close();
        updateBranch.close();
        insertHistory.close();
    }

    @Override
    public String getName() {
        return "BenchA";
    }

}
