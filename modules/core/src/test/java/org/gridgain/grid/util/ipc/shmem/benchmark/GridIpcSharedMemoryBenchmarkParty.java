/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem.benchmark;

import java.io.*;

/**
 *
 */
interface GridIpcSharedMemoryBenchmarkParty {
    /** */
    public static final int DFLT_SPACE_SIZE = 512 * 1024;

    /** */
    public static final int DFLT_BUF_SIZE = 8 * 1024;

    /** */
    public static final String DFLT_TOKEN =
        new File(System.getProperty("java.io.tmpdir"), "benchmark").getAbsolutePath();
}
