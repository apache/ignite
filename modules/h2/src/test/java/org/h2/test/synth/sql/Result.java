/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;

import org.h2.test.TestBase;
import org.h2.util.New;

/**
 * Represents an in-memory result.
 */
class Result implements Comparable<Result> {
    static final int SUCCESS = 0, BOOLEAN = 1, INT = 2, EXCEPTION = 3,
            RESULT_SET = 4;

    String sql;

    private final int type;
    private boolean bool;
    private int intValue;
    private SQLException exception;
    private ArrayList<Row> rows;
    private ArrayList<Column> header;

    Result(String sql) {
        this.sql = sql;
        type = SUCCESS;
    }

    Result(String sql, SQLException e) {
        this.sql = sql;
        type = EXCEPTION;
        exception = e;
    }

    Result(String sql, boolean b) {
        this.sql = sql;
        type = BOOLEAN;
        this.bool = b;
    }

    Result(String sql, int i) {
        this.sql = sql;
        type = INT;
        this.intValue = i;
    }

    Result(TestSynth config, String sql, ResultSet rs) {
        this.sql = sql;
        type = RESULT_SET;
        try {
            rows = New.arrayList();
            header = New.arrayList();
            ResultSetMetaData meta = rs.getMetaData();
            int len = meta.getColumnCount();
            Column[] cols = new Column[len];
            for (int i = 0; i < len; i++) {
                cols[i] = new Column(meta, i + 1);
            }
            while (rs.next()) {
                Row row = new Row(config, rs, len);
                rows.add(row);
            }
            Collections.sort(rows);
        } catch (SQLException e) {
            // type = EXCEPTION;
            // exception = e;
            TestBase.logError("error reading result set", e);
        }
    }

    @Override
    public String toString() {
        switch (type) {
        case SUCCESS:
            return "success";
        case BOOLEAN:
            return "boolean: " + this.bool;
        case INT:
            return "int: " + this.intValue;
        case EXCEPTION: {
            StringWriter w = new StringWriter();
            exception.printStackTrace(new PrintWriter(w));
            return "exception: " + exception.getSQLState() + ": " +
                    exception.getMessage() + "\r\n" + w.toString();
        }
        case RESULT_SET:
            String result = "ResultSet { // size=" + rows.size() + "\r\n  ";
            for (Column column : header) {
                result += column.toString() + "; ";
            }
            result += "} = {\r\n";
            for (Row row : rows) {
                result += "  { " + row.toString() + "};\r\n";
            }
            return result + "}";
        default:
            throw new AssertionError("type=" + type);
        }
    }

    @Override
    public int compareTo(Result r) {
        switch (type) {
        case EXCEPTION:
            if (r.type != EXCEPTION) {
                return 1;
            }
            return 0;
            // return
            // exception.getSQLState().compareTo(r.exception.getSQLState());
        case BOOLEAN:
        case INT:
        case SUCCESS:
        case RESULT_SET:
            return toString().compareTo(r.toString());
        default:
            throw new AssertionError("type=" + type);
        }
    }

//    public void log() {
//        switch (type) {
//        case SUCCESS:
//            System.out.println("> ok");
//            break;
//        case EXCEPTION:
//            System.out.println("> exception");
//            break;
//        case INT:
//            if (intValue == 0) {
//                System.out.println("> ok");
//            } else {
//                System.out.println("> update count: " + intValue);
//            }
//            break;
//        case RESULT_SET:
//            System.out.println("> rs " + rows.size());
//            break;
//        default:
//        }
//        System.out.println();
//    }

}
