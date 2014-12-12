/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Starts up one grid node (server) with pre-defined ports and tasks to test client-server interactions.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 * <p>
 * After this example has been started you can use pre-defined endpoints and task names in your
 * client-server interactions to work with the node over secured protocols (binary over SSL or https).
 * <p>
 * Usually you cannot start secured and unsecured nodes in one grid, so started together
 * secured and unsecured nodes belong to different grids.
 * <p>
 * Available endponts:
 * <ul>
 *     <li>127.0.0.1:10443 - TCP SSL-protected endpoint.</li>
 *     <li>127.0.0.1:11443 - HTTP SSL-protected endpoint.</li>
 * </ul>
 * <p>
 * Required credentials for remote client authentication: "s3cret".
 */
public class GridClientSslNodeStartup {
    /**
     * Starts up two nodes with specified cache configuration on pre-defined endpoints.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException In case of any exception.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        System.setProperty("CLIENTS_MODULE_PATH", U.resolveGridGainPath("modules/clients").getAbsolutePath());

        try (Ignite g = G.start("modules/clients/src/test/resources/spring-server-ssl-node.xml")) {
            U.sleep(Long.MAX_VALUE);
        }
    }
}
