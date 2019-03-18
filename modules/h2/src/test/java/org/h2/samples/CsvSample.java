/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.h2.store.fs.FileUtils;
import org.h2.tools.Csv;
import org.h2.tools.SimpleResultSet;

/**
 * This sample application shows how to use the CSV tool
 * to write CSV (comma separated values) files, and
 * how to use the tool to read such files.
 * See also the section CSV (Comma Separated Values) Support in the Tutorial.
 */
public class CsvSample {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws SQLException {
        CsvSample.write();
        CsvSample.read();
        FileUtils.delete("data/test.csv");
    }

    /**
     * Write a CSV file.
     */
    static void write() throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        rs.addColumn("EMAIL", Types.VARCHAR, 255, 0);
        rs.addColumn("PHONE", Types.VARCHAR, 255, 0);
        rs.addRow("Bob Meier", "bob.meier@abcde.abc", "+41123456789");
        rs.addRow("John Jones", "john.jones@abcde.abc", "+41976543210");
        new Csv().write("data/test.csv", rs, null);
    }

    /**
     * Read a CSV file.
     */
    static void read() throws SQLException {
        ResultSet rs = new Csv().read("data/test.csv", null, null);
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0; i < meta.getColumnCount(); i++) {
                System.out.println(
                        meta.getColumnLabel(i + 1) + ": " +
                        rs.getString(i + 1));
            }
            System.out.println();
        }
        rs.close();
    }

}
