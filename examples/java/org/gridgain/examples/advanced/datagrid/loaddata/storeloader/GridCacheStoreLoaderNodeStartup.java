// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.loaddata.storeloader;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.product.*;

import javax.swing.*;

import static org.gridgain.examples.basic.datagrid.CacheStoreLoaderExample.*;
import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Starts up an empty node with cache configuration that {@link GridCacheLoaderStore} configured.
 * <p>
 * Please note that this example loads large amount of data into memory and therefore
 * requires larger heap size. Please add {@code -Xmx1g} to JVM startup options.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheStoreLoaderNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        GridExamplesUtils.checkMinMemory(MIN_MEMORY);

        try (Grid grid = GridGain.start("examples/config/example-cache-storeloader.xml")) {
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[] {
                    new JLabel("GridGain started."),
                    new JLabel("Press OK to stop GridGain.")
                },
                "GridGain",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
    }
}
