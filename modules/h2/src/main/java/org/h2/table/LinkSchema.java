/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import org.h2.message.DbException;
import org.h2.tools.SimpleResultSet;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * A utility class to create table links for a whole schema.
 */
public class LinkSchema {

    private LinkSchema() {
        // utility class
    }

    /**
     * Link all tables of a schema to the database.
     *
     * @param conn the connection to the database where the links are to be
     *            created
     * @param targetSchema the schema name where the objects should be created
     * @param driver the driver class name of the linked database
     * @param url the database URL of the linked database
     * @param user the user name
     * @param password the password
     * @param sourceSchema the schema where the existing tables are
     * @return a result set with the created tables
     */
    public static ResultSet linkSchema(Connection conn, String targetSchema,
            String driver, String url, String user, String password,
            String sourceSchema) {
        Connection c2 = null;
        Statement stat = null;
        ResultSet rs = null;
        SimpleResultSet result = new SimpleResultSet();
        result.setAutoClose(false);
        result.addColumn("TABLE_NAME", Types.VARCHAR, Integer.MAX_VALUE, 0);
        try {
            c2 = JdbcUtils.getConnection(driver, url, user, password);
            stat = conn.createStatement();
            stat.execute("CREATE SCHEMA IF NOT EXISTS " +
                        StringUtils.quoteIdentifier(targetSchema));
            //Workaround for PostgreSQL to avoid index names
            if (url.startsWith("jdbc:postgresql:")) {
                rs = c2.getMetaData().getTables(null, sourceSchema, null,
                        new String[] { "TABLE", "LINKED TABLE", "VIEW", "EXTERNAL" });
            } else {
                rs = c2.getMetaData().getTables(null, sourceSchema, null, null);
            }
            while (rs.next()) {
                String table = rs.getString("TABLE_NAME");
                StringBuilder buff = new StringBuilder();
                buff.append("DROP TABLE IF EXISTS ").
                    append(StringUtils.quoteIdentifier(targetSchema)).
                    append('.').
                    append(StringUtils.quoteIdentifier(table));
                stat.execute(buff.toString());
                buff = new StringBuilder();
                buff.append("CREATE LINKED TABLE ").
                    append(StringUtils.quoteIdentifier(targetSchema)).
                    append('.').
                    append(StringUtils.quoteIdentifier(table)).
                    append('(').
                    append(StringUtils.quoteStringSQL(driver)).
                    append(", ").
                    append(StringUtils.quoteStringSQL(url)).
                    append(", ").
                    append(StringUtils.quoteStringSQL(user)).
                    append(", ").
                    append(StringUtils.quoteStringSQL(password)).
                    append(", ").
                    append(StringUtils.quoteStringSQL(sourceSchema)).
                    append(", ").
                    append(StringUtils.quoteStringSQL(table)).
                    append(')');
                stat.execute(buff.toString());
                result.addRow(table);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(c2);
            JdbcUtils.closeSilently(stat);
        }
        return result;
    }
}
