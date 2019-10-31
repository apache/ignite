/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.thread;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The operation part of {@link TestMulti}.
 * Queries and updates a table.
 */
public class TestMultiNews extends TestMultiThread {

    private static final String PREFIX_URL =
        "http://feeds.wizbangblog.com/WizbangFullFeed?m=";

    private static final int LEN = 10000;
    private Connection conn;

    TestMultiNews(TestMulti base) throws SQLException {
        super(base);
        conn = base.getConnection();
    }

    @Override
    void operation() throws SQLException {
        if (random.nextInt(10) == 0) {
            conn.close();
            conn = base.getConnection();
        } else if (random.nextInt(10) == 0) {
            if (random.nextBoolean()) {
                conn.commit();
            } else {
                conn.rollback();
            }
        } else if (random.nextInt(10) == 0) {
            conn.setAutoCommit(random.nextBoolean());
        } else {
            if (random.nextBoolean()) {
                PreparedStatement prep;
                if (random.nextBoolean()) {
                    prep = conn.prepareStatement(
                            "SELECT * FROM NEWS WHERE LINK = ?");
                } else {
                    prep = conn.prepareStatement(
                            "SELECT * FROM NEWS WHERE VALUE = ?");
                }
                prep.setString(1, PREFIX_URL + random.nextInt(LEN));
                ResultSet rs = prep.executeQuery();
                if (!rs.next()) {
                    throw new SQLException("expected one row, got none");
                }
                if (rs.next()) {
                    throw new SQLException("expected one row, got more");
                }
            } else {
                PreparedStatement prep = conn.prepareStatement(
                        "UPDATE NEWS SET STATE = ? WHERE FID = ?");
                prep.setInt(1, random.nextInt(100));
                prep.setInt(2, random.nextInt(LEN));
                int count = prep.executeUpdate();
                if (count != 1) {
                    throw new SQLException("expected one row, got " + count);
                }
            }
        }
    }

    @Override
    void begin() {
        // nothing to do
    }

    @Override
    void end() throws SQLException {
        conn.close();
    }

    @Override
    void finalTest() {
        // nothing to do
    }

    @Override
    void first() throws SQLException {
        Connection c = base.getConnection();
        Statement stat = c.createStatement();
        stat.execute("CREATE TABLE TEST (ID IDENTITY, NAME VARCHAR)");
        stat.execute("CREATE TABLE NEWS" +
                "(FID NUMERIC(19) PRIMARY KEY, COMMENTS LONGVARCHAR, " +
                "LINK VARCHAR(255), STATE INTEGER, VALUE VARCHAR(255))");
        stat.execute("CREATE INDEX IF NOT EXISTS " +
                "NEWS_GUID_VALUE_INDEX ON NEWS(VALUE)");
        stat.execute("CREATE INDEX IF NOT EXISTS " +
                "NEWS_LINK_INDEX ON NEWS(LINK)");
        stat.execute("CREATE INDEX IF NOT EXISTS " +
                "NEWS_STATE_INDEX ON NEWS(STATE)");
        PreparedStatement prep = c.prepareStatement(
                "INSERT INTO NEWS (FID, COMMENTS, LINK, STATE, VALUE) " +
                "VALUES (?, ?, ?, ?, ?) ");
        PreparedStatement prep2 = c.prepareStatement(
                "INSERT INTO TEST (NAME) VALUES (?)");
        for (int i = 0; i < LEN; i++) {
            int x = random.nextInt(10) * 128;
            StringBuilder buff = new StringBuilder();
            while (buff.length() < x) {
                buff.append("Test ");
                buff.append(buff.length());
                buff.append(' ');
            }
            String comment = buff.toString();
            // FID
            prep.setInt(1, i);
            // COMMENTS
            prep.setString(2, comment);
            // LINK
            prep.setString(3, PREFIX_URL + i);
            // STATE
            prep.setInt(4, 0);
            // VALUE
            prep.setString(5, PREFIX_URL + i);
            prep.execute();
            prep2.setString(1, comment);
            prep2.execute();
        }
    }

}
