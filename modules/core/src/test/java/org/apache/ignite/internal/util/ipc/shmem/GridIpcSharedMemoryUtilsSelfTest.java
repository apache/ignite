/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.ipc.shmem;

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
