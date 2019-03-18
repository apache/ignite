/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple wrapper around the JDBC API.
 * Currently used for testing.
 * Features:
 * <ul>
 * <li>No checked exceptions
 * </li><li>Easy to use, fluent API
 * </li></ul>
 */
public class Db {

    private Connection conn;
    private Statement stat;
    private final HashMap<String, PreparedStatement> prepared =
            new HashMap<>();

    /**
     * Create a database object using the given connection.
     *
     * @param conn the database connection
     */
    public Db(Connection conn) {
        try {
            this.conn = conn;
            stat = conn.createStatement();
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    /**
     * Prepare a SQL statement.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    public Prepared prepare(String sql) {
        try {
            PreparedStatement prep = prepared.get(sql);
            if (prep == null) {
                prep = conn.prepareStatement(sql);
                prepared.put(sql, prep);
            }
            return new Prepared(conn.prepareStatement(sql));
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    /**
     * Execute a SQL statement.
     *
     * @param sql the SQL statement
     */
    public void execute(String sql) {
        try {
            stat.execute(sql);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    /**
     * Read a result set.
     *
     * @param rs the result set
     * @return a list of maps
     */
    static List<Map<String, Object>> query(ResultSet rs) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        while (rs.next()) {
            HashMap<String, Object> map = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                map.put(meta.getColumnLabel(i+1), rs.getObject(i+1));
            }
            list.add(map);
        }
        return list;
    }

    /**
     * Execute a SQL statement.
     *
     * @param sql the SQL statement
     * @return a list of maps
     */
    public List<Map<String, Object>> query(String sql) {
        try {
            return query(stat.executeQuery(sql));
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    /**
     * Close the database connection.
     */
    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    /**
     * This class represents a prepared statement.
     */
    public static class Prepared {
        private final PreparedStatement prep;
        private int index;

        Prepared(PreparedStatement prep) {
            this.prep = prep;
        }

        /**
         * Set the value of the current parameter.
         *
         * @param x the value
         * @return itself
         */
        public Prepared set(int x) {
            try {
                prep.setInt(++index, x);
                return this;
            } catch (SQLException e) {
                throw convert(e);
            }
        }

        /**
         * Set the value of the current parameter.
         *
         * @param x the value
         * @return itself
         */
        public Prepared set(String x) {
            try {
                prep.setString(++index, x);
                return this;
            } catch (SQLException e) {
                throw convert(e);
            }
        }

        /**
         * Set the value of the current parameter.
         *
         * @param x the value
         * @return itself
         */
        public Prepared set(byte[] x) {
            try {
                prep.setBytes(++index, x);
                return this;
            } catch (SQLException e) {
                throw convert(e);
            }
        }

        /**
         * Set the value of the current parameter.
         *
         * @param x the value
         * @return itself
         */
        public Prepared set(InputStream x) {
            try {
                prep.setBinaryStream(++index, x, -1);
                return this;
            } catch (SQLException e) {
                throw convert(e);
            }
        }

        /**
         * Execute the prepared statement.
         */
        public void execute() {
            try {
                prep.execute();
            } catch (SQLException e) {
                throw convert(e);
            }
        }

        /**
         * Execute the prepared query.
         *
         * @return the result list
         */
        public List<Map<String, Object>> query() {
            try {
                return Db.query(prep.executeQuery());
            } catch (SQLException e) {
                throw convert(e);
            }
        }
    }

    /**
     * Convert a checked exception to a runtime exception.
     *
     * @param e the checked exception
     * @return the runtime exception
     */
    static RuntimeException convert(Exception e) {
        return new RuntimeException(e.toString(), e);
    }

    public void setAutoCommit(boolean autoCommit) {
        try {
            conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            throw convert(e);
        }
    }

    /**
     * Commit a pending transaction.
     */
    public void commit() {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw convert(e);
        }
    }

}
