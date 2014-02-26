// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.api;

import org.gridgain.grid.*;
import org.gridgain.grid.product.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Starts up grid node (server) for use with {@link ClientCacheExample}.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class ClientCacheExampleNodeStartup {
    /**
     * Starts up a node with cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException In case of any exception.
     */
    public static void main(String[] args) throws GridException {
        String springCfgPath = "examples/config/example-cache.xml";

        GridGain.start(springCfgPath);
    }
}
