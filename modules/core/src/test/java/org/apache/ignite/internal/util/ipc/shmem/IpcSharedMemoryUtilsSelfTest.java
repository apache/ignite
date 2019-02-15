/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.File;
import java.util.Collection;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IpcSharedMemoryUtilsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IpcSharedMemoryNativeLoader.load(log());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPid() throws Exception {
        int pid = IpcSharedMemoryUtils.pid();

        info("PID of the current process: " + pid);

        assert IpcSharedMemoryUtils.alive(pid);

        // PID cannot have this value.
        assert !IpcSharedMemoryUtils.alive(Integer.MAX_VALUE) : "Alive PID: " + Integer.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIdsGet() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestIgniteInstanceName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        IpcSharedMemorySpace space = new IpcSharedMemorySpace(tok, IpcSharedMemoryUtils.pid(), 0, 128,
            false, log);

        info("Space: " + space);

        int shmemId = space.sharedMemoryId();

        try {
            // Write some data to the space, but avoid blocking.
            space.write(new byte[] {0, 1, 2, 3}, 0, 4, 0);

            Collection<Integer> ids = IpcSharedMemoryUtils.sharedMemoryIds();

            info("IDs: " + ids);

            assertTrue(ids.contains(shmemId));
        }
        finally {
            space.forceClose();
        }

        assertFalse(IpcSharedMemoryUtils.sharedMemoryIds().contains(shmemId));
    }
}
