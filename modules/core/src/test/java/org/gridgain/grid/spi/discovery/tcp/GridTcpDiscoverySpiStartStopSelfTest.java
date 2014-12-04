/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

/**
 * Grid TCP discovery SPI start stop self test.
 */
@GridSpiTest(spi = GridTcpDiscoverySpi.class, group = "Discovery SPI")
public class GridTcpDiscoverySpiStartStopSelfTest extends GridSpiStartStopAbstractTest<GridTcpDiscoverySpi> {
    /**
     * @return IP finder.
     */
    @GridSpiTestConfig
    public GridTcpDiscoveryIpFinder getIpFinder() {
        return new GridTcpDiscoveryVmIpFinder(true);
    }

    /**
     * @return Discovery data collector.
     */
    @GridSpiTestConfig
    public GridDiscoverySpiDataExchange getDataExchange() {
        return new GridDiscoverySpiDataExchange() {
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
    public GridDiscoverySpiNodeAuthenticator getAuthenticator() {
        return new GridDiscoverySpiNodeAuthenticator() {
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
