/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.router;

import org.gridgain.grid.*;

/**
 * Starts up grid node (server) for use with {@link RouterExample} or for use
 * with .NET C# client API example as well.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 */
public class RouterExampleNodeStartup {
    /**
     * Starts up a node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException In case of failure.
     */
    public static void main(String[] args) throws GridException {
        // Enable full logging for log access in examples.
        System.setProperty(GridSystemProperties.GG_QUIET, "false");

        GridGain.start("examples/config/example-compute.xml");
    }
}
