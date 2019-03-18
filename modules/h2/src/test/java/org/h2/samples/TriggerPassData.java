/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.h2.api.Trigger;

/**
 * This sample application shows how to pass data to a trigger. Trigger data can
 * be persisted by storing it in the database.
 */
public class TriggerPassData implements Trigger {

    private static final Map<String, TriggerPassData> TRIGGERS =
        Collections.synchronizedMap(new HashMap<String, TriggerPassData>());
    private String triggerData;

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("CREATE ALIAS TRIGGER_SET FOR \"" +
                TriggerPassData.class.getName() +
                ".setTriggerData\"");
        stat.execute("CREATE TRIGGER T1 " +
                "BEFORE INSERT ON TEST " +
                "FOR EACH ROW CALL \"" +
                TriggerPassData.class.getName() + "\"");
        stat.execute("CALL TRIGGER_SET('T1', 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("CALL TRIGGER_SET('T1', 'World')");
        stat.execute("INSERT INTO TEST VALUES(2)");
        stat.close();
        conn.close();
    }

    @Override
    public void init(Connection conn, String schemaName,
            String triggerName, String tableName, boolean before,
            int type) throws SQLException {
        TRIGGERS.put(getPrefix(conn) + triggerName, this);
    }

    @Override
    public void fire(Connection conn, Object[] old, Object[] row) {
        System.out.println(triggerData + ": " + row[0]);
    }

    @Override
    public void close() {
        // ignore
    }

    @Override
    public void remove() {
        // ignore
    }

    /**
     * Call this method to change a specific trigger.
     *
     * @param conn the connection
     * @param trigger the trigger name
     * @param data the data
     */
    public static void setTriggerData(Connection conn, String trigger,
            String data) throws SQLException {
        TRIGGERS.get(getPrefix(conn) + trigger).triggerData = data;
    }

    private static String getPrefix(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "call ifnull(database_path() || '_', '') || database() || '_'");
        rs.next();
        return rs.getString(1);
    }

}
