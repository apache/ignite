/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf.context;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2.engine.SysProperties;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * Contains meta data information about a database schema.
 * This class is used by the H2 Console.
 */
public class DbSchema {

    /**
     * The schema name.
     */
    public final String name;

    /**
     * True if this is the default schema for this database.
     */
    public final boolean isDefault;

    /**
     * True if this is a system schema (for example the INFORMATION_SCHEMA).
     */
    public final boolean isSystem;

    /**
     * The quoted schema name.
     */
    public final String quotedName;

    /**
     * The database content container.
     */
    private final DbContents contents;

    /**
     * The table list.
     */
    private DbTableOrView[] tables;

    /**
     * The procedures list.
     */
    private DbProcedure[] procedures;

    DbSchema(DbContents contents, String name, boolean isDefault) {
        this.contents = contents;
        this.name = name;
        this.quotedName = contents.quoteIdentifier(name);
        this.isDefault = isDefault;
        if (name == null) {
            // firebird
            isSystem = true;
        } else if ("INFORMATION_SCHEMA".equals(name)) {
            isSystem = true;
        } else if (!contents.isH2() &&
                StringUtils.toUpperEnglish(name).startsWith("INFO")) {
            isSystem = true;
        } else if (contents.isPostgreSQL() &&
                StringUtils.toUpperEnglish(name).startsWith("PG_")) {
            isSystem = true;
        } else if (contents.isDerby() && name.startsWith("SYS")) {
            isSystem = true;
        } else {
            isSystem = false;
        }
    }

    /**
     * @return The database content container.
     */
    public DbContents getContents() {
        return contents;
    }

    /**
     * @return The table list.
     */
    public DbTableOrView[] getTables() {
        return tables;
    }

    /**
     * @return The procedure list.
     */
    public DbProcedure[] getProcedures() {
        return procedures;
    }

    /**
     * Read all tables for this schema from the database meta data.
     *
     * @param meta the database meta data
     * @param tableTypes the table types to read
     */
    public void readTables(DatabaseMetaData meta, String[] tableTypes)
            throws SQLException {
        ResultSet rs = meta.getTables(null, name, null, tableTypes);
        ArrayList<DbTableOrView> list = New.arrayList();
        while (rs.next()) {
            DbTableOrView table = new DbTableOrView(this, rs);
            if (contents.isOracle() && table.getName().indexOf('$') > 0) {
                continue;
            }
            list.add(table);
        }
        rs.close();
        tables = list.toArray(new DbTableOrView[0]);
        if (tables.length < SysProperties.CONSOLE_MAX_TABLES_LIST_COLUMNS) {
            for (DbTableOrView tab : tables) {
                try {
                    tab.readColumns(meta);
                } catch (SQLException e) {
                    // MySQL:
                    // View '...' references invalid table(s) or column(s)
                    // or function(s) or definer/invoker of view
                    // lack rights to use them HY000/1356
                    // ignore
                }
            }
        }
    }

    /**
     * Read all procedures in the dataBase.
     * @param meta the database meta data
     * @throws SQLException Error while fetching procedures
     */
    public void readProcedures(DatabaseMetaData meta) throws SQLException {
        ResultSet rs = meta.getProcedures(null, name, null);
        ArrayList<DbProcedure> list = New.arrayList();
        while (rs.next()) {
            list.add(new DbProcedure(this, rs));
        }
        rs.close();
        procedures = list.toArray(new DbProcedure[0]);
        if (procedures.length < SysProperties.CONSOLE_MAX_PROCEDURES_LIST_COLUMNS) {
            for (DbProcedure procedure : procedures) {
                procedure.readParameters(meta);
            }
        }
    }
}
