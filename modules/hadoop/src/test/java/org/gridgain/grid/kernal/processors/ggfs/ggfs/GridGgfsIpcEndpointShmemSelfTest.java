/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs.ggfs;

/**
 * Tests for IPC endpoint configured with shared memory.
 */
public class GridGgfsIpcEndpointShmemSelfTest extends GridGgfsIpcEndpointAbstractSelfTest {
    /**
     * Constructor.
     */
    public GridGgfsIpcEndpointShmemSelfTest() {
        super(false);
    }
}
