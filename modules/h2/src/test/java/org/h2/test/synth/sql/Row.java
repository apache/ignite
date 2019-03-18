/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Represents a row.
 */
class Row implements Comparable<Row> {
    private final Value[] data;

    public Row(TestSynth config, ResultSet rs, int len) throws SQLException {
        data = new Value[len];
        for (int i = 0; i < len; i++) {
            data[i] = Value.read(config, rs, i + 1);
        }
    }

    @Override
    public String toString() {
        String s = "";
        for (Object o : data) {
            s += o == null ? "NULL" : o.toString();
            s += "; ";
        }
        return s;
    }

    @Override
    public int compareTo(Row r2) {
        int result = 0;
        for (int i = 0; i < data.length && result == 0; i++) {
            Object o1 = data[i];
            Object o2 = r2.data[i];
            if (o1 == null) {
                result = (o2 == null) ? 0 : -1;
            } else if (o2 == null) {
                result = 1;
            } else {
                result = o1.toString().compareTo(o2.toString());
            }
        }
        return result;
    }

}
