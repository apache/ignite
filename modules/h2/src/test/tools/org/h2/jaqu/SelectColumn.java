/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import org.h2.jaqu.TableDefinition.FieldDefinition;

/**
 * This class represents a column of a table in a query.
 *
 * @param <T> the table data type
 */
class SelectColumn<T> {
    private final SelectTable<T> selectTable;
    private final FieldDefinition fieldDef;

    SelectColumn(SelectTable<T> table, FieldDefinition fieldDef) {
        this.selectTable = table;
        this.fieldDef = fieldDef;
    }

    void appendSQL(SQLStatement stat) {
        if (selectTable.getQuery().isJoin()) {
            stat.appendSQL(selectTable.getAs() + "." + fieldDef.columnName);
        } else {
            stat.appendSQL(fieldDef.columnName);
        }
    }

    FieldDefinition getFieldDefinition() {
        return fieldDef;
    }

    SelectTable<T> getSelectTable() {
        return selectTable;
    }

    Object getCurrentValue() {
        return fieldDef.getValue(selectTable.getCurrent());
    }
}
