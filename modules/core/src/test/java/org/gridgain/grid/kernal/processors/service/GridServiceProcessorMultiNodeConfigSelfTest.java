// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.service.*;

/**
 * Single node services test.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceProcessorMultiNodeConfigSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridServiceConfiguration[] services() {
        GridServiceConfiguration[] arr = new GridServiceConfiguration[2];

        GridServiceConfiguration cfg = new GridServiceConfiguration();

        cfg.setName("serviceConfigSingleton");
        cfg.setMaxPerNodeCount(1);
        cfg.setTotalCount(1);

        arr[0] = cfg;

        cfg = new GridServiceConfiguration();

        cfg.setName("serviceConfigEachNode");
        cfg.setMaxPerNodeCount(1);

        arr[1] = cfg;

        return arr;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingletonUpdateTopology() throws Exception {
        checkSingletonUpdateTopology("serviceConfigSingleton");
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNodeUpdateTopology() throws Exception {
        checkDeployOnEachNodeUpdateTopology("serviceConfigEachNode");
    }

    /**
     * @throws Exception If failed.
     */
    public void testAll() throws Exception {
        checkSingletonUpdateTopology("serviceConfigAllSingleton");

        DummyService.reset();

        checkDeployOnEachNodeUpdateTopology("serviceConfigAllEachNode");
    }
}
