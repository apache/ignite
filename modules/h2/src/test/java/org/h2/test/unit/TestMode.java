/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.engine.Mode;
import org.h2.test.TestBase;

/**
 * Unit test for the Mode class.
 */
public class TestMode extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testDb2ClientInfo();
        testDerbyClientInfo();
        testHsqlDbClientInfo();
        testMsSqlServerClientInfo();
        testMySqlClientInfo();
        testOracleClientInfo();
        testPostgresqlClientInfo();
    }

    private void testDb2ClientInfo() {
        Mode db2Mode = Mode.getInstance("DB2");

        assertTrue(db2Mode.supportedClientInfoPropertiesRegEx.matcher(
                "ApplicationName").matches());
        assertTrue(db2Mode.supportedClientInfoPropertiesRegEx.matcher(
                "ClientAccountingInformation").matches());
        assertTrue(db2Mode.supportedClientInfoPropertiesRegEx.matcher(
                "ClientUser").matches());
        assertTrue(db2Mode.supportedClientInfoPropertiesRegEx.matcher(
                "ClientCorrelationToken").matches());

        assertFalse(db2Mode.supportedClientInfoPropertiesRegEx.matcher(
                "AnyOtherValue").matches());
    }

    private void testDerbyClientInfo() {
        Mode derbyMode = Mode.getInstance("Derby");
        assertNull(derbyMode.supportedClientInfoPropertiesRegEx);
    }

    private void testHsqlDbClientInfo() {
        Mode hsqlMode = Mode.getInstance("HSQLDB");
        assertNull(hsqlMode.supportedClientInfoPropertiesRegEx);
    }

    private void testMsSqlServerClientInfo() {
        Mode msSqlMode = Mode.getInstance("MSSQLServer");
        assertNull(msSqlMode.supportedClientInfoPropertiesRegEx);
    }

    private void testMySqlClientInfo() {
        Mode mySqlMode = Mode.getInstance("MySQL");
        assertTrue(mySqlMode.supportedClientInfoPropertiesRegEx.matcher(
                "AnyString").matches());
    }

    private void testOracleClientInfo() {
        Mode oracleMode = Mode.getInstance("Oracle");
        assertTrue(oracleMode.supportedClientInfoPropertiesRegEx.matcher(
                "anythingContaining.aDot").matches());
        assertFalse(oracleMode.supportedClientInfoPropertiesRegEx.matcher(
                "anythingContainingNoDot").matches());
    }


    private void testPostgresqlClientInfo() {
        Mode postgresqlMode = Mode.getInstance("PostgreSQL");
        assertTrue(postgresqlMode.supportedClientInfoPropertiesRegEx.matcher(
                "ApplicationName").matches());
        assertFalse(postgresqlMode.supportedClientInfoPropertiesRegEx.matcher(
                "AnyOtherValue").matches());
    }

}
