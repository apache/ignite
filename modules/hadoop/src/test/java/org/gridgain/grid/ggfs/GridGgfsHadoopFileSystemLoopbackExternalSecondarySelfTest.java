/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import static org.gridgain.grid.ggfs.IgniteFsMode.*;

/**
 * GGFS Hadoop file system IPC loopback self test in SECONDARY mode.
 */
public class GridGgfsHadoopFileSystemLoopbackExternalSecondarySelfTest extends
    GridGgfsHadoopFileSystemLoopbackAbstractSelfTest {

    /**
     * Constructor.
     */
    public GridGgfsHadoopFileSystemLoopbackExternalSecondarySelfTest() {
        super(PROXY, true);
    }
}
