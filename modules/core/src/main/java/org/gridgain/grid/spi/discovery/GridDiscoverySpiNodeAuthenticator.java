/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.security.*;

/**
 * Node authenticator.
 */
public interface GridDiscoverySpiNodeAuthenticator {
    /**
     * Security credentials.
     *
     * @param node Node to authenticate.
     * @param cred Security credentials.
     * @return Security context if authentication succeeded or {@code null} if authentication failed.
     * @throws GridException If authentication process failed
     *      (invalid credentials should not lead to this exception).
     */
    public GridSecurityContext authenticateNode(ClusterNode node, GridSecurityCredentials cred) throws GridException;

    /**
     * Gets global node authentication flag.
     *
     * @return {@code True} if all nodes in topology should authenticate joining node, {@code false} if only
     *      coordinator should do the authentication.
     */
    public boolean isGlobalNodeAuthentication();
}
