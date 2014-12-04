/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.mapper;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Continuous mapper load test.
 */
public class GridContinuousMapperLoadTest1 {
    /**
     * Main method.
     *
     * @param args Parameters.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = G.start("examples/config/example-cache.xml")) {
            int max = 30000;

            IgniteDataLoader<Integer, TestObject> ldr = g.dataLoader("replicated");

            for (int i = 0; i < max; i++)
                ldr.addData(i, new TestObject(i, "Test object: " + i));

            // Wait for loader to complete.
            ldr.close(false);

            X.println("Populated replicated cache.");

            g.compute().execute(new GridContinuousMapperTask1(), max);
        }
    }
}
