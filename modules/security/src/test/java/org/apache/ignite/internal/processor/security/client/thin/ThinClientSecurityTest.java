package org.apache.ignite.internal.processor.security.client.thin;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractSecurityTest;
import org.apache.ignite.internal.processor.security.TestSecurityData;
import org.apache.ignite.internal.processor.security.TestSecurityPluginConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityPermission;

public class ThinClientSecurityTest extends AbstractSecurityTest {

    private static final String CLIENT = "client";

    protected IgniteConfiguration getConfiguration(TestSecurityData... clientData) throws Exception {
        return getConfiguration(G.allGrids().size(), clientData);
    }

    protected IgniteConfiguration getConfiguration(int idx,
        TestSecurityData... clientData) throws Exception {

        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(instanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setPluginConfigurations(
                new TestSecurityPluginConfiguration()
                    .setLogin("srv_" + instanceName)
                    .setPermissions(allowAll())
                    .clientSecData(clientData)
            );
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(
            getConfiguration(
                new TestSecurityData(CLIENT, "", builder().build())
            )
        ).cluster().active(true);
    }

    public void test() throws Exception {
        grid(0).getOrCreateCache("TEST_CACHE").put("key", "value for test");

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
            .setUserName("client")
            .setUserPassword(""))) {

            System.out.println(
                client.cache("TEST_CACHE").get("key")
            );
        }

        /*try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
            .setUserName("client_2")
            .setUserPassword("123"))) {

            System.out.println(
                client.cache("TEST_CACHE").get("key")
            );
        }*/
    }

}
