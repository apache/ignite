/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

import org.h2.jaqu.Table.JQColumn;
import org.h2.jaqu.Table.JQTable;

/**
 * A system table to track database and table versions.
 */
@JQTable(name = "_jq_versions",
        primaryKey = "schemaName tableName", memoryTable = true)
public class DbVersion {

    @JQColumn(name = "schemaName", allowNull = false)
    String schema = "";

    @JQColumn(name = "tableName", allowNull = false)
    String table = "";

    @JQColumn(name = "version")
    Integer version;

    public DbVersion() {
        // nothing to do
    }

    /**
     * Constructor for defining a version entry. Both the schema and the table
     * are empty strings, which means this is the row for the 'database'.
     *
     * @param version the database version
     */
    public DbVersion(int version) {
        this.schema = "";
        this.table = "";
        this.version = version;
    }

}
