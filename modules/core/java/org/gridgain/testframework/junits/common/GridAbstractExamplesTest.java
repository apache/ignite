/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.common;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Base class for examples test.
 */
public abstract class GridAbstractExamplesTest extends GridCommonAbstractTest {
    /** */
    protected static final String[] EMPTY_ARGS = new String[0];

    /** */
    protected static final int RMT_NODES_CNT = 3;

    /** */
    protected static final String RMT_NODE_CFGS = "modules/core/src/test/config/examples.properties";

    /** */
    protected static final String DFLT_CFG = "examples/config/example-compute.xml";

    /** */
    private static final Properties rmtCfgs = new Properties();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Starts remote nodes.
     *
     * @throws Exception If failed.
     */
    protected final void startRemoteNodes() throws Exception {
        String name = getName().replaceFirst("test", "");

        if (rmtCfgs.isEmpty()) {
            info("Loading remote configs properties from file: " + RMT_NODE_CFGS);

            try (FileReader reader = new FileReader(new File(U.getGridGainHome(), RMT_NODE_CFGS))) {
                rmtCfgs.load(reader);
            }
        }

        String cfg = rmtCfgs.getProperty(name, defaultConfig());

        info("Config for remote nodes [name=" + name + ", cfg=" + cfg + ", dflt=" + defaultConfig() + "]");

        for (int i = 0; i < RMT_NODES_CNT; i++)
            startGrid(getTestGridName(i), cfg);
    }

    /**
     * @return Default config for this test.
     */
    protected String defaultConfig() {
        return DFLT_CFG;
    }
}
