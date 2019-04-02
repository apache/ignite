package org.apache.ignite.client;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.Test;

/**
 * Checks if it can connect to a valid address from the node address list.
 */
public class ConnectionTest {
    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testEmptyNodeAddress() throws Exception {
        testConnection("");
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testNullNodeAddress() throws Exception {
        testConnection(null);
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientException.class)
    public void testNullNodeAddresses() throws Exception {
        testConnection(null, null);
    }

    /** */
    @Test
    public void testValidNodeAddresses() throws Exception {
        testConnection(Config.SERVER);
    }

    /** */
    @Test(expected = org.apache.ignite.client.ClientConnectionException.class)
    public void testInvalidNodeAddresses() throws Exception {
        testConnection("127.0.0.1:47500", "127.0.0.1:10801");
    }

    /** */
    @Test
    public void testValidInvalidNodeAddressesMix() throws Exception {
        testConnection("127.0.0.1:47500", "127.0.0.1:10801", Config.SERVER);
    }

    /**
     * @param addrs Addresses to connect.
     */
    private void testConnection(String... addrs) throws Exception {
        try (LocalIgniteCluster cluster = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(new ClientConfiguration()
                     .setAddresses(addrs))) {
        }
    }
}
