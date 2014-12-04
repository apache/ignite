/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.configuration.*;

/**
 * Configuration validation tests.
 */
public class GridHadoopValidationSelfTest extends GridHadoopAbstractSelfTest {
    /** Peer class loading enabled flag. */
    public boolean peerClassLoading;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        peerClassLoading = false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(peerClassLoading);

        return cfg;
    }

    /**
     * Ensure that Grid starts when all configuration parameters are valid.
     *
     * @throws Exception If failed.
     */
    public void testValid() throws Exception {
        startGrids(1);
    }
}
