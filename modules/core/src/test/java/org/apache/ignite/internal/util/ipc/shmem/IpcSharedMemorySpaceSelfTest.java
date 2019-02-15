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
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IpcSharedMemorySpaceSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int DATA_LEN = 1024 * 1024;

    /** */
    private static final byte[] DATA = new byte[DATA_LEN];

    /**
     *
     */
    static {
        for (int i = 0; i < DATA_LEN; i++)
            DATA[i] = (byte)i;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IpcSharedMemoryNativeLoader.load(log());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBasicOperations() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), UUID.randomUUID().toString());

        assert tokFile.createNewFile();

        final String tok = tokFile.getAbsolutePath();

        info("Array length: " + DATA.length);

        final AtomicReference<IpcSharedMemorySpace> spaceRef = new AtomicReference<>();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @SuppressWarnings("TooBroadScope")
                @Override public Object call() throws Exception {
                    try (IpcSharedMemorySpace space = new IpcSharedMemorySpace(tok, 0, 0, 128, false,
                        log)) {
                        spaceRef.set(space);

                        int bytesWritten = 0;

                        for (; ; ) {
                            int len = Math.min(DATA.length - bytesWritten,
                                ThreadLocalRandom.current().nextInt(256) + 1);

                            space.write(DATA, bytesWritten, len, 0);

                            bytesWritten += len;

                            if (bytesWritten == DATA.length)
                                break;
                        }

                        info("Thread finished.");

                        return null;
                    }
                }
            },
            1,
            "writer");

        IgniteInternalFuture<?> fut2 = multithreadedAsync(
            new Callable<Object>() {
                @SuppressWarnings({"TooBroadScope"})
                @Override public Object call() throws Exception {
                    IpcSharedMemorySpace inSpace;

                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return spaceRef.get() != null;
                        }
                    }, 10_000);

                    inSpace = spaceRef.get();

                    assertNotNull(inSpace);

                    try (IpcSharedMemorySpace space = new IpcSharedMemorySpace(tok, 0, 0, 128, true,
                        inSpace.sharedMemoryId(), log)) {
                        byte[] buf = new byte[DATA_LEN];

                        int bytesRead = 0;

                        for (; ; ) {
                            int len = Math.min(DATA.length - bytesRead,
                                ThreadLocalRandom.current().nextInt(32) + 1);

                            int len0 = space.read(buf, bytesRead, len, 0);

                            assert len0 > 0;

                            bytesRead += len0;

                            if (bytesRead == DATA_LEN)
                                break;
                        }

                        assertTrue(Arrays.equals(DATA, buf));

                        return null;
                    }
                }
            },
            1,
            "reader");

        fut1.get();
        fut2.get();

        assert !tokFile.exists();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testForceClose() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestIgniteInstanceName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        info("Using token file: " + tok);

        Collection<Integer> ids = IpcSharedMemoryUtils.sharedMemoryIds();

        info("IDs in the system: " + ids);

        IpcSharedMemorySpace space = new IpcSharedMemorySpace(tok, IpcSharedMemoryUtils.pid(), 0, 128,
            false, log);

        ids = IpcSharedMemoryUtils.sharedMemoryIds();

        info("IDs in the system: " + ids);

        assert ids.contains(space.sharedMemoryId());

        // Write some data to the space, but avoid blocking.
        space.write(DATA, 0, 16, 0);

        int shmemId = space.sharedMemoryId();

        space.forceClose();

        ids = IpcSharedMemoryUtils.sharedMemoryIds();

        info("IDs in the system: " + ids);

        assert !ids.contains(shmemId);

        assert !tokFile.exists();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadAfterClose() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestIgniteInstanceName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        info("Using token file: " + tok);

        IpcSharedMemorySpace spaceOut = new IpcSharedMemorySpace(tok, IpcSharedMemoryUtils.pid(), 0, 128,
            false, log);

        try (IpcSharedMemorySpace spaceIn = new IpcSharedMemorySpace(tok, IpcSharedMemoryUtils.pid(), 0,
            128, true, spaceOut.sharedMemoryId(), log)) {
            // Write some data to the space, but avoid blocking.
            spaceOut.write(DATA, 0, 16, 0);

            spaceOut.close();

            // Read after other party has already called "close()".
            // Space has data available and should read it.
            byte[] buf = new byte[16];

            int len = spaceIn.read(buf, 0, 16, 0);

            assert len == 16;

            len = spaceIn.read(buf, 0, 16, 0);

            assert len == -1;
        }

        assert !tokFile.exists();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWriteAfterClose() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestIgniteInstanceName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        info("Using token file: " + tok);

        try (IpcSharedMemorySpace spaceOut = new IpcSharedMemorySpace(tok, IpcSharedMemoryUtils.pid(),
            IpcSharedMemoryUtils.pid(), 128, false, log)) {

            try (IpcSharedMemorySpace spaceIn = new IpcSharedMemorySpace(tok, IpcSharedMemoryUtils.pid(),
                IpcSharedMemoryUtils.pid(), 128, true, spaceOut.sharedMemoryId(), log)) {
                // Write some data to the space, but avoid blocking.
                spaceOut.write(DATA, 0, 16, 0);

                spaceIn.close();

                try {
                    spaceOut.write(DATA, 0, 16, 0);

                    assert false;
                }
                catch (IgniteCheckedException e) {
                    info("Caught expected exception: " + e);
                }
            }
        }

        assert !tokFile.exists();
    }
}
