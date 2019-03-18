/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.tools.SimpleResultSet;

/**
 * User defined functions can return a result set,
 * and can therefore be used like a table.
 * This sample application uses such a function to convert
 * polar to cartesian coordinates.
 */
public class FunctionMultiReturn {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS P2C " +
                "FOR \"org.h2.samples.FunctionMultiReturn.polar2Cartesian\" ");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT X, Y FROM P2C(?, ?)");
        prep.setDouble(1, 5.0);
        prep.setDouble(2, 0.5);
        ResultSet rs = prep.executeQuery();
        while (rs.next()) {
            double x = rs.getDouble(1);
            double y = rs.getDouble(2);
            System.out.println("result: (x=" + x + ", y="+y+")");
        }

        stat.execute("CREATE TABLE TEST(ID IDENTITY, R DOUBLE, A DOUBLE)");
        stat.execute("INSERT INTO TEST(R, A) VALUES(5.0, 0.5), (10.0, 0.6)");
        stat.execute("CREATE ALIAS P2C_SET " +
                "FOR \"org.h2.samples.FunctionMultiReturn.polar2CartesianSet\" ");
        rs = conn.createStatement().executeQuery(
                "SELECT * FROM P2C_SET('SELECT * FROM TEST')");
        while (rs.next()) {
            double r = rs.getDouble("R");
            double a = rs.getDouble("A");
            double x = rs.getDouble("X");
            double y = rs.getDouble("Y");
            System.out.println("(r="+r+" a="+a+") :" +
                    " (x=" + x + ", y="+y+")");
        }

        stat.execute("CREATE ALIAS P2C_A " +
                "FOR \"org.h2.samples.FunctionMultiReturn.polar2CartesianArray\" ");
        rs = conn.createStatement().executeQuery(
                "SELECT R, A, P2C_A(R, A) FROM TEST");
        while (rs.next()) {
            double r = rs.getDouble(1);
            double a = rs.getDouble(2);
            Object o = rs.getObject(3);
            Object[] xy = (Object[]) o;
            double x = ((Double) xy[0]).doubleValue();
            double y = ((Double) xy[1]).doubleValue();
            System.out.println("(r=" + r + " a=" + a + ") :" +
                    " (x=" + x + ", y=" + y + ")");
        }

        rs = stat.executeQuery(
                "SELECT R, A, ARRAY_GET(E, 1), ARRAY_GET(E, 2) " +
                "FROM (SELECT R, A, P2C_A(R, A) E FROM TEST)");
        while (rs.next()) {
            double r = rs.getDouble(1);
            double a = rs.getDouble(2);
            double x = rs.getDouble(3);
            double y = rs.getDouble(4);
            System.out.println("(r="+r+" a="+a+") :" +
                    " (x=" + x + ", y="+y+")");
        }
        rs.close();

        prep.close();
        conn.close();
    }

    /**
     * Convert polar coordinates to cartesian coordinates. The function may be
     * called twice, once to retrieve the result columns (with null parameters),
     * and the second time to return the data.
     *
     * @param r the distance from the point 0/0
     * @param alpha the angle
     * @return a result set with two columns: x and y
     */
    public static ResultSet polar2Cartesian(Double r, Double alpha) {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("X", Types.DOUBLE, 0, 0);
        rs.addColumn("Y", Types.DOUBLE, 0, 0);
        if (r != null && alpha != null) {
            double x = r.doubleValue() * Math.cos(alpha.doubleValue());
            double y = r.doubleValue() * Math.sin(alpha.doubleValue());
            rs.addRow(x, y);
        }
        return rs;
    }

    /**
     * Convert polar coordinates to cartesian coordinates. The function may be
     * called twice, once to retrieve the result columns (with null parameters),
     * and the second time to return the data.
     *
     * @param r the distance from the point 0/0
     * @param alpha the angle
     * @return an array two values: x and y
     */
    public static Object[] polar2CartesianArray(Double r, Double alpha) {
        double x = r.doubleValue() * Math.cos(alpha.doubleValue());
        double y = r.doubleValue() * Math.sin(alpha.doubleValue());
        return new Object[]{x, y};
    }

    /**
     * Convert a set of polar coordinates to cartesian coordinates. The function
     * may be called twice, once to retrieve the result columns (with null
     * parameters), and the second time to return the data.
     *
     * @param conn the connection
     * @param query the query
     * @return a result set with the coordinates
     */
    public static ResultSet polar2CartesianSet(Connection conn, String query)
            throws SQLException {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("R", Types.DOUBLE, 0, 0);
        result.addColumn("A", Types.DOUBLE, 0, 0);
        result.addColumn("X", Types.DOUBLE, 0, 0);
        result.addColumn("Y", Types.DOUBLE, 0, 0);
        if (query != null) {
            ResultSet rs = conn.createStatement().executeQuery(query);
            while (rs.next()) {
                double r = rs.getDouble("R");
                double alpha = rs.getDouble("A");
                double x = r * Math.cos(alpha);
                double y = r * Math.sin(alpha);
                result.addRow(r, alpha, x, y);
            }
        }
        return result;
    }

}
