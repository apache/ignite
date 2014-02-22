// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.misc.client.api;

import org.gridgain.grid.*;
import org.gridgain.grid.product.*;

import javax.swing.*;

import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Starts up grid node (server) for use with {@link GridClientCacheExample}.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridClientCacheExampleNodeStartup {
    /**
     * Change to {@code true} to enable SSL.
     * Note that you need to appropriately update router and client configurations.
     */
    static final Boolean SSL_ENABLED = false;

    /**
     * Starts up a node with cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException In case of any exception.
     */
    public static void main(String[] args) throws GridException {
        String springCfgPath = SSL_ENABLED ? "examples/config/example-cache-client-ssl.xml" :
            "examples/config/example-cache-client.xml";

        try (Grid g = GridGain.start(springCfgPath)) {
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[]{
                    new JLabel("GridGain started."),
                    new JLabel("Press OK to stop GridGain.")
                },
                "GridGain",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
    }
}
