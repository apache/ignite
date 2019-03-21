/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.h2.util.Utils;
import org.h2.value.Value;

/**
 * Merged result. Used to combine several results into one. Merged result will
 * contain rows from all appended results. Results are not required to have the
 * same lists of columns, but required to have compatible column definitions,
 * for example, if one result has a {@link java.sql.Types#VARCHAR} column
 * {@code NAME} then another results that have {@code NAME} column should also
 * define it with the same type.
 */
public final class MergedResult {
    private final ArrayList<Map<SimpleResult.Column, Value>> data = Utils.newSmallArrayList();

    private final ArrayList<SimpleResult.Column> columns = Utils.newSmallArrayList();

    /**
     * Appends a result.
     *
     * @param result
     *            result to append
     */
    public void add(ResultInterface result) {
        int count = result.getVisibleColumnCount();
        if (count == 0) {
            return;
        }
        SimpleResult.Column[] cols = new SimpleResult.Column[count];
        for (int i = 0; i < count; i++) {
            SimpleResult.Column c = new SimpleResult.Column(result.getAlias(i), result.getColumnName(i),
                    result.getColumnType(i));
            cols[i] = c;
            if (!columns.contains(c)) {
                columns.add(c);
            }
        }
        while (result.next()) {
            if (count == 1) {
                data.add(Collections.singletonMap(cols[0], result.currentRow()[0]));
            } else {
                HashMap<SimpleResult.Column, Value> map = new HashMap<>();
                for (int i = 0; i < count; i++) {
                    SimpleResult.Column ci = cols[i];
                    map.put(ci, result.currentRow()[i]);
                }
                data.add(map);
            }
        }
    }

    /**
     * Returns merged results.
     *
     * @return result with rows from all appended result sets
     */
    public SimpleResult getResult() {
        SimpleResult result = new SimpleResult();
        for (SimpleResult.Column c : columns) {
            result.addColumn(c);
        }
        for (Map<SimpleResult.Column, Value> map : data) {
            Value[] row = new Value[columns.size()];
            for (Map.Entry<SimpleResult.Column, Value> entry : map.entrySet()) {
                row[columns.indexOf(entry.getKey())] = entry.getValue();
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public String toString() {
        return columns + ": " + data.size();
    }

}
