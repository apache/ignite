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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

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
                                ThreadLocalRandom8.current().nextInt(256) + 1);

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
                @SuppressWarnings({"TooBroadScope", "StatementWithEmptyBody"})
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
                                ThreadLocalRandom8.current().nextInt(32) + 1);

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
    public void testForceClose() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestGridName());

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
    public void testReadAfterClose() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestGridName());

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
    public void testWriteAfterClose() throws Exception {
        File tokFile = new File(IgniteSystemProperties.getString("java.io.tmpdir"), getTestGridName());

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