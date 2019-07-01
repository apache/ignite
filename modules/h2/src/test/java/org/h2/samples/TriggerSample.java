/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.Trigger;

/**
 * This sample application shows how to use database triggers.
 */
public class TriggerSample {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE INVOICE(ID INT PRIMARY KEY, AMOUNT DECIMAL)");
        stat.execute("CREATE TABLE INVOICE_SUM(AMOUNT DECIMAL)");
        stat.execute("INSERT INTO INVOICE_SUM VALUES(0.0)");

        stat.execute("CREATE TRIGGER INV_INS " +
                "AFTER INSERT ON INVOICE FOR EACH ROW " +
                "CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
        stat.execute("CREATE TRIGGER INV_UPD " +
                "AFTER UPDATE ON INVOICE FOR EACH ROW " +
                "CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
        stat.execute("CREATE TRIGGER INV_DEL " +
                "AFTER DELETE ON INVOICE FOR EACH ROW " +
                "CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");

        stat.execute("INSERT INTO INVOICE VALUES(1, 10.0)");
        stat.execute("INSERT INTO INVOICE VALUES(2, 19.95)");
        stat.execute("UPDATE INVOICE SET AMOUNT=20.0 WHERE ID=2");
        stat.execute("DELETE FROM INVOICE WHERE ID=1");

        ResultSet rs;
        rs = stat.executeQuery("SELECT AMOUNT FROM INVOICE_SUM");
        rs.next();
        System.out.println("The sum is " + rs.getBigDecimal(1));
        rs.close();
        stat.close();
        conn.close();
    }

    /**
     * This class is a simple trigger implementation.
     */
    public static class MyTrigger implements Trigger {

        /**
         * Initializes the trigger.
         *
         * @param conn a connection to the database
         * @param schemaName the name of the schema
         * @param triggerName the name of the trigger used in the CREATE TRIGGER
         *            statement
         * @param tableName the name of the table
         * @param before whether the fire method is called before or after the
         *            operation is performed
         * @param type the operation type: INSERT, UPDATE, or DELETE
         */
        @Override
        public void init(Connection conn, String schemaName,
                String triggerName, String tableName, boolean before, int type) {
            // initialize the trigger object is necessary
        }

        /**
         * This method is called for each triggered action.
         *
         * @param conn a connection to the database
         * @param oldRow the old row, or null if no old row is available (for
         *            INSERT)
         * @param newRow the new row, or null if no new row is available (for
         *            DELETE)
         * @throws SQLException if the operation must be undone
         */
        @Override
        public void fire(Connection conn,
                Object[] oldRow, Object[] newRow)
                throws SQLException {
            BigDecimal diff = null;
            if (newRow != null) {
                diff = (BigDecimal) newRow[1];
            }
            if (oldRow != null) {
                BigDecimal m = (BigDecimal) oldRow[1];
                diff = diff == null ? m.negate() : diff.subtract(m);
            }
            PreparedStatement prep = conn.prepareStatement(
                    "UPDATE INVOICE_SUM SET AMOUNT=AMOUNT+?");
            prep.setBigDecimal(1, diff);
            prep.execute();
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void remove() {
            // ignore
        }

    }

}
