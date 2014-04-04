/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

/**
 *
 */
public class GridJettyRestProcessorUnsignedSelfTest extends GridJettyRestProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int restPort() {
        return 8091;
    }

    /**
     * @return Signature.
     * @throws Exception If failed.
     */
    @Override protected String signature() throws Exception {
        return null;
    }
}
