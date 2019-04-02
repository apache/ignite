/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf.context;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Contains meta data information about a table or a view.
 * This class is used by the H2 Console.
 */
public class DbTableOrView {

    /**
     * The schema this table belongs to.
     */
    private final DbSchema schema;

    /**
     * The table name.
     */
    private final String name;

    /**
     * The quoted table name.
     */
    private final String quotedName;

    /**
     * True if this represents a view.
     */
    private final boolean isView;

    /**
     * The column list.
     */
    private DbColumn[] columns;

    public DbTableOrView(DbSchema schema, ResultSet rs) throws SQLException {
        this.schema = schema;
        name = rs.getString("TABLE_NAME");
        String type = rs.getString("TABLE_TYPE");
        isView = "VIEW".equals(type);
        quotedName = schema.getContents().quoteIdentifier(name);
    }

    /**
     * @return The schema this table belongs to.
     */
    public DbSchema getSchema() {
        return schema;
    }

    /**
     * @return The column list.
     */
    public DbColumn[] getColumns() {
        return columns;
    }

    /**
     * @return The table name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return True if this represents a view.
     */
    public boolean isView() {
        return isView;
    }

    /**
     * @return The quoted table name.
     */
    public String getQuotedName() {
        return quotedName;
    }

    /**
     * Read the column for this table from the database meta data.
     *
     * @param meta the database meta data
     * @param ps prepared statement with custom query for H2 database, null for
     *           others
     */
    public void readColumns(DatabaseMetaData meta, PreparedStatement ps) throws SQLException {
        ResultSet rs;
        if (schema.getContents().isH2()) {
            ps.setString(1, schema.name);
            ps.setString(2, name);
            rs = ps.executeQuery();
        } else {
            rs = meta.getColumns(null, schema.name, name, null);
        }
        ArrayList<DbColumn> list = new ArrayList<>();
        while (rs.next()) {
            DbColumn column = DbColumn.getColumn(schema.getContents(), rs);
            list.add(column);
        }
        rs.close();
        columns = list.toArray(new DbColumn[0]);
    }

}
