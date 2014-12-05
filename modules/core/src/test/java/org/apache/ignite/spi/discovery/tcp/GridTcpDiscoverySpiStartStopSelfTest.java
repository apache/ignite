/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.apache.ignite.plugin.security.*;
import org.gridgain.grid.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

/**
 * Grid TCP discovery SPI start stop self test.
 */
@GridSpiTest(spi = TcpDiscoverySpi.class, group = "Discovery SPI")
public class GridTcpDiscoverySpiStartStopSelfTest extends GridSpiStartStopAbstractTest<TcpDiscoverySpi> {
    /**
     * @return IP finder.
     */
    @GridSpiTestConfig
    public TcpDiscoveryIpFinder getIpFinder() {
        return new TcpDiscoveryVmIpFinder(true);
    }

    /**
     * @return Discovery data collector.
     */
    @GridSpiTestConfig
    public DiscoverySpiDataExchange getDataExchange() {
        return new DiscoverySpiDataExchange() {
            @Override public List<Object> collect(UUID nodeId) {
                return null;
            }

            @Override public void onExchange(List<Object> data) {
                // No-op.
            }
        };
    }

    /**
     * Discovery SPI authenticator.
     *
     * @return Authenticator.
     */
    @GridSpiTestConfig
    public DiscoverySpiNodeAuthenticator getAuthenticator() {
        return new DiscoverySpiNodeAuthenticator() {
            @Override public GridSecurityContext authenticateNode(ClusterNode n, GridSecurityCredentials cred) {
                GridSecuritySubjectAdapter subj = new GridSecuritySubjectAdapter(
                    GridSecuritySubjectType.REMOTE_NODE, n.id());

                subj.permissions(new GridAllowAllPermissionSet());

                return new GridSecurityContext(subj);
            }

            @Override public boolean isGlobalNodeAuthentication() {
                return false;
            }
        };
    }
}
