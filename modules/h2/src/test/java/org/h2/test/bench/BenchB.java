/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

/**
 * This test is similar to the TPC-B test of the Transaction Processing Council
 * (TPC). Multiple threads are used (one thread per connection). Referential
 * integrity is not implemented.
 * <p>
 * See also http://www.tpc.org/tpcb
 */
public class BenchB implements Bench, Runnable {

    private static final int SCALE = 4;
    private static final int BRANCHES = 1;
    private static final int TELLERS = 10;
    private static final int ACCOUNTS = 100000;

    private int threadCount = 10;

    // master data
    private Database database;
    private int transactionPerClient;

    // client data
    private BenchB master;
    private Connection conn;
    private PreparedStatement updateAccount;
    private PreparedStatement selectAccount;
    private PreparedStatement updateTeller;
    private PreparedStatement updateBranch;
    private PreparedStatement insertHistory;
    private Random random;

    public BenchB() {
        // nothing to do
    }

    private BenchB(BenchB master, int seed) throws SQLException {
        this.master = master;
        random = new Random(seed);
        conn = master.database.openNewConnection();
        conn.setAutoCommit(false);
        updateAccount = conn.prepareStatement(
                "UPDATE ACCOUNTS SET ABALANCE=ABALANCE+? WHERE AID=?");
        selectAccount = conn.prepareStatement(
                "SELECT ABALANCE FROM ACCOUNTS WHERE AID=?");
        updateTeller = conn.prepareStatement(
                "UPDATE TELLERS SET TBALANCE=TBALANCE+? WHERE TID=?");
        updateBranch = conn.prepareStatement(
                "UPDATE BRANCHES SET BBALANCE=BBALANCE+? WHERE BID=?");
        insertHistory = conn.prepareStatement(
                "INSERT INTO HISTORY(TID, BID, AID, DELTA) VALUES(?, ?, ?, ?)");
    }

    @Override
    public void init(Database db, int size) throws SQLException {
        this.database = db;
        this.transactionPerClient = getTransactionsPerClient(size);

        db.start(this, "Init");
        db.openConnection();
        db.dropTable("BRANCHES");
        db.dropTable("TELLERS");
        db.dropTable("ACCOUNTS");
        db.dropTable("HISTORY");
        String[] create = {
                "CREATE TABLE BRANCHES(" +
                        "BID INT NOT NULL PRIMARY KEY, " +
                        "BBALANCE INT, FILLER VARCHAR(88))",
                "CREATE TABLE TELLERS(" +
                        "TID INT NOT NULL PRIMARY KEY, " +
                        "BID INT, TBALANCE INT, FILLER VARCHAR(84))",
                "CREATE TABLE ACCOUNTS(" +
                        "AID INT NOT NULL PRIMARY KEY, " +
                        "BID INT, ABALANCE INT, FILLER VARCHAR(84))",
                "CREATE TABLE HISTORY(" +
                        "TID INT, BID INT, AID INT, " +
                        "DELTA INT, TIME DATETIME, FILLER VARCHAR(22))" };
        for (String sql : create) {
            db.update(sql);
        }
        PreparedStatement prep;
        db.setAutoCommit(false);
        int commitEvery = 1000;
        prep = db.prepare(
                "INSERT INTO BRANCHES(BID, BBALANCE) VALUES(?, 0)");
        for (int i = 0; i < BRANCHES * SCALE; i++) {
            prep.setInt(1, i);
            db.update(prep, "insertBranches");
            if (i % commitEvery == 0) {
                db.commit();
            }
        }
        db.commit();
        prep = db.prepare(
                "INSERT INTO TELLERS(TID, BID, TBALANCE) VALUES(?, ?, 0)");
        for (int i = 0; i < TELLERS * SCALE; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i / TELLERS);
            db.update(prep, "insertTellers");
            if (i % commitEvery == 0) {
                db.commit();
            }
        }
        db.commit();
        int len = ACCOUNTS * SCALE;
        prep = db.prepare(
                "INSERT INTO ACCOUNTS(AID, BID, ABALANCE) VALUES(?, ?, 0)");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i / ACCOUNTS);
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

    /**
     * Get the number of transactions per client.
     *
     * @param size test size
     * @return the transactions per client
     */
    protected int getTransactionsPerClient(int size) {
        return size / 8;
    }

    @Override
    public void run() {
        int accountsPerBranch = ACCOUNTS / BRANCHES;
        for (int i = 0; i < master.transactionPerClient; i++) {
            int branch = random.nextInt(BRANCHES);
            int teller = random.nextInt(TELLERS);
            int account;
            if (random.nextInt(100) < 85) {
                account = random.nextInt(accountsPerBranch) + branch * accountsPerBranch;
            } else {
                account = random.nextInt(ACCOUNTS);
            }
            int delta = random.nextInt(1000);
            doOne(branch, teller, account, delta);
        }
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    private void doOne(int branch, int teller, int account, int delta) {
        try {
            // UPDATE ACCOUNTS SET ABALANCE=ABALANCE+? WHERE AID=?
            updateAccount.setInt(1, delta);
            updateAccount.setInt(2, account);
            master.database.update(updateAccount, "UpdateAccounts");
            updateAccount.executeUpdate();

            // SELECT ABALANCE FROM ACCOUNTS WHERE AID=?
            selectAccount.setInt(1, account);
            ResultSet rs = master.database.query(selectAccount);
            while (rs.next()) {
                rs.getInt(1);
            }

            // UPDATE TELLERS SET TBALANCE=TABLANCE+? WHERE TID=?
            updateTeller.setInt(1, delta);
            updateTeller.setInt(2, teller);
            master.database.update(updateTeller, "UpdateTeller");

            // UPDATE BRANCHES SET BBALANCE=BBALANCE+? WHERE BID=?
            updateBranch.setInt(1, delta);
            updateBranch.setInt(2, branch);
            master.database.update(updateBranch, "UpdateBranch");

            // INSERT INTO HISTORY(TID, BID, AID, DELTA) VALUES(?, ?, ?, ?)
            insertHistory.setInt(1, teller);
            insertHistory.setInt(2, branch);
            insertHistory.setInt(3, account);
            insertHistory.setInt(4, delta);
            master.database.update(insertHistory, "InsertHistory");
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void runTest() throws Exception {
        Database db = database;
        db.start(this, "Transactions");
        db.openConnection();
        processTransactions();
        db.closeConnection();
        db.end();
        db.openConnection();
        processTransactions();
        db.logMemory(this, "Memory Usage");
        db.closeConnection();
    }

    private void processTransactions() throws Exception {
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(new BenchB(this, i), "BenchB-" + i);
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }

    @Override
    public String getName() {
        return "BenchB";
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
