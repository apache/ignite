/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import org.h2.command.Prepared;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Represents a prepared statement.
 */
public class H2Statement extends H2Program {

    H2Statement(Prepared prepared) {
        super(prepared);
    }

    /**
     * Execute the statement.
     */
    public void execute() {
        if (prepared.isQuery()) {
            prepared.query(0);
        } else {
            prepared.update();
        }
    }

    /**
     * Execute the insert statement and return the id of the inserted row.
     *
     * @return the id of the inserted row
     */
    public long executeInsert() {
        return prepared.update();
    }

    /**
     * Execute the query and return the value of the first column and row as a
     * long.
     *
     * @return the value
     */
    public long simpleQueryForLong() {
        return simpleQuery().getLong();
    }

    /**
     * Execute the query and return the value of the first column and row as a
     * string.
     *
     * @return the value
     */
    public String simpleQueryForString() {
        return simpleQuery().getString();
    }

    private Value simpleQuery() {
        ResultInterface result = prepared.query(1);
        try {
            if (result.next()) {
                Value[] row = result.currentRow();
                if (row.length > 0) {
                    return row[0];
                }
            }
        } finally {
            result.close();
        }
        return ValueNull.INSTANCE;
    }

}
