/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.test.auth;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import javax.sql.DataSource;

import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.security.auth.DefaultAuthenticator;
import org.h2.security.auth.H2AuthConfig;
import org.h2.security.auth.H2AuthConfigXml;
import org.h2.security.auth.impl.AssignRealmNameRole;
import org.h2.security.auth.impl.JaasCredentialsValidator;
import org.h2.security.auth.impl.StaticRolesMapper;
import org.h2.security.auth.impl.StaticUserCredentialsValidator;
import org.h2.test.TestBase;

/**
 * Test for custom authentication.
 */
public class TestAuthentication extends TestBase {

    private static final String TESTXML = "<h2Auth allowUserRegistration=\"true\" createMissingRoles=\"false\">"
            + "<realm name=\"ciao\" validatorClass=\"myclass\"/>"
            + "<realm name=\"miao\" validatorClass=\"myclass1\">"
            + "<property name=\"prop1\" value=\"value1\"/>"
            + "<userToRolesMapper className=\"class1\">"
            + "<property name=\"prop2\" value=\"value2\"/>"
            + "</userToRolesMapper>"
            + "</realm>"
            + "</h2Auth>";

    private static final String JAAS_CONFIG_NAME = "testJaasH2";

    private String externalUserPassword;
    private DefaultAuthenticator defaultAuthenticator;
    private Session session;
    private Database database;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    /**
     * Gets external user password.
     *
     * @return external user password.
     */
    String getExternalUserPassword() {
        if (externalUserPassword == null) {
            externalUserPassword = UUID.randomUUID().toString();
        }
        return externalUserPassword;
    }

    private static String getRealmName() {
        return "testRealm";
    }

    private static String getStaticRoleName() {
        return "staticRole";
    }

    private void configureAuthentication(Database database) {
        defaultAuthenticator = new DefaultAuthenticator(true);
        defaultAuthenticator.setAllowUserRegistration(true);
        defaultAuthenticator.setCreateMissingRoles(true);
        defaultAuthenticator.addRealm(getRealmName(), new JaasCredentialsValidator(JAAS_CONFIG_NAME));
        defaultAuthenticator.addRealm(getRealmName() + "_STATIC",
                new StaticUserCredentialsValidator("staticuser[0-9]", "staticpassword"));
        defaultAuthenticator.setUserToRolesMappers(new AssignRealmNameRole("@%s"),
                new StaticRolesMapper(getStaticRoleName()));
        database.setAuthenticator(defaultAuthenticator);
    }

    private void configureJaas() {
        final Configuration innerConfiguration = Configuration.getConfiguration();
        Configuration.setConfiguration(new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                if (name.equals(JAAS_CONFIG_NAME)) {
                    HashMap<String, String> options = new HashMap<>();
                    options.put("password", getExternalUserPassword());
                    return new AppConfigurationEntry[] { new AppConfigurationEntry(MyLoginModule.class.getName(),
                            LoginModuleControlFlag.REQUIRED, options) };
                }
                return innerConfiguration.getAppConfigurationEntry(name);
            }
        });
    }

    private String getDatabaseURL() {
        return "jdbc:h2:mem:" + getClass().getSimpleName();
    }

    private static String getExternalUser() {
        return "user";
    }

    @Override
    public void test() throws Exception {
        Configuration oldConfiguration = Configuration.getConfiguration();
        try {
            configureJaas();
            Properties properties = new Properties();
            properties.setProperty("USER", "dba");
            ConnectionInfo connectionInfo = new ConnectionInfo(getDatabaseURL(), properties);
            session = Engine.getInstance().createSession(connectionInfo);
            database = session.getDatabase();
            configureAuthentication(database);
            try {
                allTests();
            } finally {
                session.close();
            }
        } finally {
            Configuration.setConfiguration(oldConfiguration);
        }
    }

    private void allTests() throws Exception {
        testInvalidPassword();
        testExternalUserWithoutRealm();
        testExternalUser();
        testAssignRealNameRole();
        testStaticRole();
        testStaticUserCredentials();
        testUserRegistration();
        testSet();
        testDatasource();
        testXmlConfig();
    }

    private void testInvalidPassword() throws Exception {
        try {
            Connection wrongLoginConnection = DriverManager.getConnection(
                    getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(), "");
            wrongLoginConnection.close();
            throw new Exception("user should not be able to login with an invalid password");
        } catch (SQLException ignored) {
        }
    }

    private void testExternalUserWithoutRealm() throws Exception {
        try {
            Connection wrongLoginConnection = DriverManager.getConnection(getDatabaseURL(), getExternalUser(),
                    getExternalUserPassword());
            wrongLoginConnection.close();
            throw new Exception("user should not be able to login without a realm");
        } catch (SQLException ignored) {
        }
    }

    private void testExternalUser() throws Exception {
        try (Connection ignored = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword())) {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
        }
    }

    private void testDatasource() throws Exception {
        DataSource dataSource = JdbcConnectionPool.create(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword());
        try (Connection ignored = dataSource.getConnection()) {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
        }
    }

    private void testAssignRealNameRole() throws Exception {
        String realmNameRoleName = "@" + getRealmName().toUpperCase();
        Role realmNameRole = database.findRole(realmNameRoleName);
        if (realmNameRole == null) {
            realmNameRole = new Role(database, database.allocateObjectId(), realmNameRoleName, false);
            session.getDatabase().addDatabaseObject(session, realmNameRole);
            session.commit(false);
        }
        try (Connection ignored = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword())) {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
            assertTrue(user.isRoleGranted(realmNameRole));
        }
    }

    private void testStaticRole() throws Exception {
        try (Connection ignored = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), getExternalUser(),
                getExternalUserPassword())) {
            User user = session.getDatabase().findUser((getExternalUser() + "@" + getRealmName()).toUpperCase());
            assertNotNull(user);
            Role staticRole = session.getDatabase().findRole(getStaticRoleName());
            if (staticRole != null) {
                assertTrue(user.isRoleGranted(staticRole));
            }
        }
    }

    private void testUserRegistration() throws Exception {
        boolean initialValueAllow = defaultAuthenticator.isAllowUserRegistration();
        defaultAuthenticator.setAllowUserRegistration(false);
        try {
            try {
                Connection wrongLoginConnection = DriverManager.getConnection(
                        getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), "___" + getExternalUser(),
                        "");
                wrongLoginConnection.close();
                throw new Exception(
                        "unregistered external users should not be able to login when allowUserRegistration=false");
            } catch (SQLException ignored) {
            }
            String validUserName = "new_" + getExternalUser();
            User validUser = new User(database, database.allocateObjectId(),
                    (validUserName.toUpperCase() + "@" + getRealmName()).toUpperCase(), false);
            validUser.setUserPasswordHash(new byte[] {});
            database.addDatabaseObject(session, validUser);
            session.commit(false);
            Connection connectionWithRegisterUser = DriverManager.getConnection(
                    getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase(), validUserName,
                    getExternalUserPassword());
            connectionWithRegisterUser.close();
        } finally {
            defaultAuthenticator.setAllowUserRegistration(initialValueAllow);
        }
    }

    private void testStaticUserCredentials() throws Exception {
        String userName="STATICUSER3";
        try (Connection ignored = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHREALM=" + getRealmName().toUpperCase() + "_STATIC", userName,
                "staticpassword")) {
            User user = session.getDatabase().findUser(userName + "@" + getRealmName().toUpperCase() + "_STATIC");
            assertNotNull(user);
        }
    }

    private void testSet() throws Exception{
        try (Connection ignored = DriverManager.getConnection(
                getDatabaseURL() + ";AUTHENTICATOR=FALSE", "DBA", "")) {
            try {
                testExternalUser();
                throw new Exception("External user shouldn't be allowed");
            } catch (Exception e) {
            }
        } finally {
            configureAuthentication(database);
        }
        testExternalUser();
    }

    private void testXmlConfig() throws Exception {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(TESTXML.getBytes());
        H2AuthConfig config = H2AuthConfigXml.parseFrom(inputStream);
        assertTrue(config.isAllowUserRegistration());
        assertFalse(config.isCreateMissingRoles());
        assertEquals("ciao",config.getRealms().get(0).getName());
        assertEquals("myclass",config.getRealms().get(0).getValidatorClass());
        assertEquals("prop1",config.getRealms().get(1).getProperties().get(0).getName());
        assertEquals("value1",config.getRealms().get(1).getProperties().get(0).getValue());
        assertEquals("class1",config.getUserToRolesMappers().get(0).getClassName());
        assertEquals("prop2",config.getUserToRolesMappers().get(0).getProperties().get(0).getName());
        assertEquals("value2",config.getUserToRolesMappers().get(0).getProperties().get(0).getValue());
    }
}