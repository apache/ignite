package org.apache.ignite.spi.discovery.tcp;

import java.net.InetAddress;
import java.net.ServerSocket;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

/**
 * SSL enabled version of {@link TcpDiscoveryWithWrongServerTest}.
 */
public class TcpDiscoveryWithWrongSslServerTest extends TcpDiscoveryWithWrongServerTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSslContextFactory(GridTestUtils.sslFactory());
    }

    /** {@inheritDoc} */
    @Override protected @NotNull ServerSocket getServerSocket(int port) throws Exception {
        return GridTestUtils.sslContext()
            .getServerSocketFactory()
            .createServerSocket(port, 10, InetAddress.getByName("127.0.0.1"));
    }
}
