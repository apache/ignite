/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import static org.apache.ignite.fs.IgniteFsMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;

/**
 * Tests for PRIMARY mode and OFFHEAP_VALUES memory.
 */
public class GridGgfsPrimaryOffheapValuesSelfTest extends GridGgfsAbstractSelfTest {
    /**
     * Constructor.
     */
    public GridGgfsPrimaryOffheapValuesSelfTest() {
        super(PRIMARY, OFFHEAP_VALUES);
    }
}
