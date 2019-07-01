/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
package org.h2.test.utils;

import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.test.TestBase;
import org.h2.util.ColumnNamer;

/**
 * Tests the column name factory.
 */
public class TestColumnNamer extends TestBase {

    private String[] ids = new String[] { "ABC", "123", "a\n2", "a$c%d#e@f!.", null,
            "VERYVERYVERYVERYVERYVERYLONGVERYVERYVERYVERYVERYVERYLONGVERYVERYVERYVERYVERYVERYLONG", "'!!!'", "'!!!!'",
            "3.1415", "\r", "col1", "col1", "col1",
            "col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2",
            "col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2col2" };

    private String[] expectedColumnName = { "ABC", "123", "a2", "acdef", "colName6", "VERYVERYVERYVERYVERYVERYLONGVE",
            "colName8", "colName9", "31415", "colName11", "col1", "col1_2", "col1_3", "col2col2col2col2col2col2col2co",
            "col2col2col2col2col2col2col2_2" };

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String[] args) {
        new TestColumnNamer().test();
    }

    @Override
    public void test() {
        ColumnNamer columnNamer = new ColumnNamer(null);
        columnNamer.getConfiguration().configure("MAX_IDENTIFIER_LENGTH = 30");
        columnNamer.getConfiguration().configure("REGULAR_EXPRESSION_MATCH_ALLOWED = '[A-Za-z0-9_]+'");
        columnNamer.getConfiguration().configure("REGULAR_EXPRESSION_MATCH_DISALLOWED = '[^A-Za-z0-9_]+'");
        columnNamer.getConfiguration().configure("DEFAULT_COLUMN_NAME_PATTERN = 'colName$$'");
        columnNamer.getConfiguration().configure("GENERATE_UNIQUE_COLUMN_NAMES = 1");

        int index = 0;
        for (String id : ids) {
            Expression columnExp = ValueExpression.getDefault();
            String newColumnName = columnNamer.getColumnName(columnExp, index + 1, id);
            assertTrue(newColumnName != null);
            assertTrue(newColumnName.length() <= 30);
            assertTrue(newColumnName.length() >= 1);
            assertEquals(newColumnName, expectedColumnName[index]);
            index++;
        }
    }
}
