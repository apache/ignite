/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Class for gathering and processing of generated keys.
 */
public final class GeneratedKeys {
    /**
     * Data for result set with generated keys.
     */
    private final ArrayList<Map<Column, Value>> data = New.arrayList();

    /**
     * Columns with generated keys in the current row.
     */
    private final ArrayList<Column> row = New.arrayList();

    /**
     * All columns with generated keys.
     */
    private final ArrayList<Column> allColumns = New.arrayList();

    /**
     * Request for keys gathering. {@code false} if generated keys are not needed,
     * {@code true} if generated keys should be configured automatically,
     * {@code int[]} to specify column indices to return generated keys from, or
     * {@code String[]} to specify column names to return generated keys from.
     */
    private Object generatedKeysRequest;

    /**
     * Processed table.
     */
    private Table table;

    /**
     * Remembers columns with generated keys.
     *
     * @param column
     *            table column
     */
    public void add(Column column) {
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return;
        }
        row.add(column);
    }

    /**
     * Clears all information from previous runs and sets a new request for
     * gathering of generated keys.
     *
     * @param generatedKeysRequest
     *            {@code false} if generated keys are not needed, {@code true} if
     *            generated keys should be configured automatically, {@code int[]}
     *            to specify column indices to return generated keys from, or
     *            {@code String[]} to specify column names to return generated keys
     *            from
     */
    public void clear(Object generatedKeysRequest) {
        this.generatedKeysRequest = generatedKeysRequest;
        data.clear();
        row.clear();
        allColumns.clear();
        table = null;
    }

    /**
     * Saves row with generated keys if any.
     *
     * @param tableRow
     *            table row that was inserted
     */
    public void confirmRow(Row tableRow) {
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return;
        }
        int size = row.size();
        if (size > 0) {
            if (size == 1) {
                Column column = row.get(0);
                data.add(Collections.singletonMap(column, tableRow.getValue(column.getColumnId())));
                if (!allColumns.contains(column)) {
                    allColumns.add(column);
                }
            } else {
                HashMap<Column, Value> map = new HashMap<>();
                for (Column column : row) {
                    map.put(column, tableRow.getValue(column.getColumnId()));
                    if (!allColumns.contains(column)) {
                        allColumns.add(column);
                    }
                }
                data.add(map);
            }
            row.clear();
        }
    }

    /**
     * Returns generated keys.
     *
     * @param session
     *            session
     * @return local result with generated keys
     */
    public LocalResult getKeys(Session session) {
        Database db = session == null ? null : session.getDatabase();
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            clear(null);
            return new LocalResult();
        }
        ArrayList<ExpressionColumn> expressionColumns;
        if (Boolean.TRUE.equals(generatedKeysRequest)) {
            expressionColumns = new ArrayList<>(allColumns.size());
            for (Column column : allColumns) {
                expressionColumns.add(new ExpressionColumn(db, column));
            }
        } else if (generatedKeysRequest instanceof int[]) {
            if (table != null) {
                int[] indices = (int[]) generatedKeysRequest;
                Column[] columns = table.getColumns();
                int cnt = columns.length;
                allColumns.clear();
                expressionColumns = new ArrayList<>(indices.length);
                for (int idx : indices) {
                    if (idx >= 1 && idx <= cnt) {
                        Column column = columns[idx - 1];
                        expressionColumns.add(new ExpressionColumn(db, column));
                        allColumns.add(column);
                    }
                }
            } else {
                clear(null);
                return new LocalResult();
            }
        } else if (generatedKeysRequest instanceof String[]) {
            if (table != null) {
                String[] names = (String[]) generatedKeysRequest;
                allColumns.clear();
                expressionColumns = new ArrayList<>(names.length);
                for (String name : names) {
                    Column column;
                    search: if (table.doesColumnExist(name)) {
                        column = table.getColumn(name);
                    } else {
                        name = StringUtils.toUpperEnglish(name);
                        if (table.doesColumnExist(name)) {
                            column = table.getColumn(name);
                        } else {
                            for (Column c : table.getColumns()) {
                                if (c.getName().equalsIgnoreCase(name)) {
                                    column = c;
                                    break search;
                                }
                            }
                            continue;
                        }
                    }
                    expressionColumns.add(new ExpressionColumn(db, column));
                    allColumns.add(column);
                }
            } else {
                clear(null);
                return new LocalResult();
            }
        } else {
            clear(null);
            return new LocalResult();
        }
        int columnCount = expressionColumns.size();
        if (columnCount == 0) {
            clear(null);
            return new LocalResult();
        }
        LocalResult result = new LocalResult(session, expressionColumns.toArray(new Expression[0]), columnCount);
        for (Map<Column, Value> map : data) {
            Value[] row = new Value[columnCount];
            for (Map.Entry<Column, Value> entry : map.entrySet()) {
                int idx = allColumns.indexOf(entry.getKey());
                if (idx >= 0) {
                    row[idx] = entry.getValue();
                }
            }
            for (int i = 0; i < columnCount; i++) {
                if (row[i] == null) {
                    row[i] = ValueNull.INSTANCE;
                }
            }
            result.addRow(row);
        }
        clear(null);
        return result;
    }

    /**
     * Initializes processing of the specified table. Should be called after
     * {@code clear()}, but before other methods.
     *
     * @param table
     *            table
     */
    public void initialize(Table table) {
        this.table = table;
    }

    /**
     * Clears unsaved information about previous row, if any. Should be called
     * before processing of a new row if previous row was not confirmed or simply
     * always before each row.
     */
    public void nextRow() {
        row.clear();
    }

    @Override
    public String toString() {
        return allColumns + ": " + data.size();
    }

}
