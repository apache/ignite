package org.apache.ignite.internal.jdbc.thin;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link ConnectionPropertiesImpl} unit tests.
 */
public class ConnectionPropertiesTest {

    /**
     * Test check the {@link ConnectionPropertiesImpl#getDriverPropertyInfo()} return properties with prefix {@link
     * ConnectionPropertiesImpl#PROP_PREFIX}
     */
    @Test
    public void testNamePrefixDriverPropertyInfo() {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();
        DriverPropertyInfo[] propsInfo = connProps.getDriverPropertyInfo();

        String propName = propsInfo[0].name;

        assertTrue(propName.startsWith(ConnectionPropertiesImpl.PROP_PREFIX));
    }

    /**
     * Test check the {@link ConnectionPropertiesImpl#init(String, Properties)} requires property names
     * with prefix {@link ConnectionPropertiesImpl#PROP_PREFIX}
     */
    @Test
    public void testPrefixedPropertiesApplicable() throws SQLException {
        final String TEST_PROP_NAME = ConnectionPropertiesImpl.PROP_PREFIX + "enforceJoinOrder";
        final Properties props = new Properties();

        props.setProperty(TEST_PROP_NAME, Boolean.TRUE.toString());
        ConnectionPropertiesImpl connPropsTrue = new ConnectionPropertiesImpl();
        connPropsTrue.init("", props);

        props.setProperty(TEST_PROP_NAME, Boolean.FALSE.toString());
        ConnectionPropertiesImpl connPropsFalse = new ConnectionPropertiesImpl();
        connPropsFalse.init("", props);

        assertTrue(connPropsTrue.isEnforceJoinOrder());
        assertFalse(connPropsFalse.isEnforceJoinOrder());
    }
}
