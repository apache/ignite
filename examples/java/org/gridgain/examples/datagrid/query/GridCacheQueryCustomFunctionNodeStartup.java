// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.query;

import org.gridgain.grid.*;

import javax.swing.*;

/**
 * Starts up an empty node with cache configuration required for
 * {@link GridCacheQueryCustomFunctionExample} example.
 * <p>
 * {@link GridCacheQueryCustomFunctionExample} should always use this class to startup standalone
 * nodes. The reason is that when running this class from IDE, it has all example classes on the classpath,
 * while running from command line doesn't. If you choose to start nodes from command line,
 * then {@link GridCacheQueryCustomFunctionExample} class should be added to class path of all nodes.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheQueryCustomFunctionNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws org.gridgain.grid.GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g =  GridGain.start("examples/config/example-cache-custom-functions.xml")) {
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
