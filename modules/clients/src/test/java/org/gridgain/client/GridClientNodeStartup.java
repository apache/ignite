/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

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
 * client-server interactions to work with the node over un-secure protocols (binary or http).
 * <p>
 * Usually you cannot start secured and unsecured nodes in one grid, so started together
 * secured and unsecured nodes belong to different grids.
 * <p>
 * Available endponts:
 * <ul>
 *     <li>127.0.0.1:10080 - TCP unsecured endpoint.</li>
 *     <li>127.0.0.1:11080 - HTTP unsecured endpoint.</li>
 * </ul>
 * <p>
 * Required credentials for remote client authentication: "s3cret".
 */
public class GridClientNodeStartup {
    /**
     * Starts up two nodes with specified cache configuration on pre-defined endpoints.
     *
     * @param args Command line arguments, none required.
     * @throws GridException In case of any exception.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = G.start("modules/clients/src/test/resources/spring-server-node.xml")) {
            U.sleep(Long.MAX_VALUE);
        }
    }
}
