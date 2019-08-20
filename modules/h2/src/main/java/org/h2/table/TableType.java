/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

/**
 * The table types.
 */
public enum TableType {

    /**
     * The table type name for linked tables.
     */
    TABLE_LINK,

    /**
     * The table type name for system tables. (aka. MetaTable)
     */
    SYSTEM_TABLE,

    /**
     * The table type name for regular data tables.
     */
    TABLE,

    /**
     * The table type name for views.
     */
    VIEW,

    /**
     * The table type name for external table engines.
     */
    EXTERNAL_TABLE_ENGINE;

    @Override
    public String toString() {
        if (this == EXTERNAL_TABLE_ENGINE) {
            return "EXTERNAL";
        } else if (this == SYSTEM_TABLE) {
            return "SYSTEM TABLE";
        } else if (this == TABLE_LINK) {
            return "TABLE LINK";
        } else {
            return super.toString();
        }
    }

}
