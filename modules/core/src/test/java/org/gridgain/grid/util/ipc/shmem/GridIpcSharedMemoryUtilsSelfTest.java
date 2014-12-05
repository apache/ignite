/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.apache.ignite.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridIpcSharedMemoryUtilsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridIpcSharedMemoryNativeLoader.load();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPid() throws Exception {
        int pid = GridIpcSharedMemoryUtils.pid();

        info("PID of the current process: " + pid);

        assert GridIpcSharedMemoryUtils.alive(pid);

        // PID cannot have this value.
        assert !GridIpcSharedMemoryUtils.alive(Integer.MAX_VALUE) : "Alive PID: " + Integer.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIdsGet() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestGridName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        GridIpcSharedMemorySpace space = new GridIpcSharedMemorySpace(tok, GridIpcSharedMemoryUtils.pid(), 0, 128,
            false, log);

        info("Space: " + space);

        int shmemId = space.sharedMemoryId();

        try {
            // Write some data to the space, but avoid blocking.
            space.write(new byte[] {0, 1, 2, 3}, 0, 4, 0);

            Collection<Integer> ids = GridIpcSharedMemoryUtils.sharedMemoryIds();

            info("IDs: " + ids);

            assertTrue(ids.contains(shmemId));
        }
        finally {
            space.forceClose();
        }

        assertFalse(GridIpcSharedMemoryUtils.sharedMemoryIds().contains(shmemId));
    }
}
