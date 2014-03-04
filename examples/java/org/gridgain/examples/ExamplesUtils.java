/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.grid.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class ExamplesUtils {
    /**
     * Exits with code {@code -1} if maximum memory is below 90% of minimally allowed threshold.
     *
     * @param min Minimum memory threshold.
     */
    public static void checkMinMemory(long min) {
        long maxMem = Runtime.getRuntime().maxMemory();

        if (maxMem < .85 * min) {
            System.err.println("Heap limit is too low (" + (maxMem / (1024 * 1024)) +
                "MB), please increase heap size at least up to " + (min / (1024 * 1024)) + "MB.");

            System.exit(-1);
        }
    }

    /**
     * Checks minimum topology size for running a certain example.
     *
     * @param prj Projection to check size for.
     * @param size Minimum number of nodes required to run a certain example.
     * @return {@code True} if check passed, {@code false} otherwise.
     */
    public static boolean checkMinTopologySize(GridProjection prj, int size) {
        int prjSize = prj.nodes().size();

        if (prjSize < size) {
            System.out.println();
            System.out.println(">>> Please start at least " + size + " grid nodes to run example.");
            System.out.println();

            return false;
        }

        return true;
    }

    /**
     * @return Resolved GridGain home via system or environment properties.
     * @throws RuntimeException If failed to resolve.
     */
    public static String resolveGridGainHome() throws RuntimeException {
        String var = System.getProperty("GRIDGAIN_HOME");

        if (var == null)
            var = System.getenv("GRIDGAIN_HOME");

        if (var == null)
            throw new RuntimeException("Failed to resolve GridGain home folder " +
                "(please set 'GRIDGAIN_HOME' environment or system variable)");

        return var;
    }
}
