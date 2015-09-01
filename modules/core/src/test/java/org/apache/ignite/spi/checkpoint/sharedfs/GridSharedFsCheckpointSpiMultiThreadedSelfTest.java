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

package org.apache.ignite.spi.checkpoint.sharedfs;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;
import org.jetbrains.annotations.Nullable;

/**
 * Tests SPI in multi-threaded environment.
 */
@GridSpiTest(spi = SharedFsCheckpointSpi.class, group = "Checkpoint SPI")
public class GridSharedFsCheckpointSpiMultiThreadedSelfTest extends
    GridSpiAbstractTest<SharedFsCheckpointSpi> {
    /** */
    private static final String PATH = "work/cp/test-shared-fs-multi-threaded";

    /** */
    private static final String CHECK_POINT_KEY = "testCheckpoint";

    /** */
    private static final int ARRAY_SIZE = 1024 * 1024;

    /** */
    private static final int ITER_CNT = 100;

    /** */
    private static final int THREAD_CNT = 10;

    /**
     * @return Paths.
     */
    @GridSpiTestConfig(setterName="setDirectoryPaths")
    public Collection<String> getDirectoryPaths() {
        return Collections.singleton(PATH);
    }

    /**
     *
     * @throws Exception If failed.
     */
    public void testSpi() throws Exception {
        final AtomicInteger writeFinished = new AtomicInteger();

        final AtomicBoolean fail = new AtomicBoolean();

        IgniteInternalFuture fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    try {
                        byte[] state = createTestArray((byte)1);

                        for (int i = 0; i < ITER_CNT; i++)
                            getSpi().saveCheckpoint(CHECK_POINT_KEY, state, 0, true);

                        return null;
                    }
                    finally {
                        writeFinished.incrementAndGet();
                    }
                }
            },
            THREAD_CNT,
            "writer-1"
        );

        IgniteInternalFuture fut2 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    try{
                        byte[] state = createTestArray((byte)2);

                        for (int i = 0; i < ITER_CNT; i++)
                            getSpi().saveCheckpoint(CHECK_POINT_KEY, state, 0, true);

                        return null;
                    }
                    finally {
                        writeFinished.incrementAndGet();
                    }
                }
            },
            THREAD_CNT,
            "writer-2"
        );

        IgniteInternalFuture fut3 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (writeFinished.get() < THREAD_CNT * 2) {
                        try {
                            byte[] state = getSpi().loadCheckpoint(CHECK_POINT_KEY);

                            if (state == null)
                                continue;

                            assert state.length == ARRAY_SIZE;

                            boolean has1 = false;
                            boolean has2 = false;

                            for (int j = 0; j < ARRAY_SIZE; j++) {
                                switch (state[j]) {
                                    case 1:
                                        has1 = true;

                                        assert !has2;

                                        break;

                                    case 2:
                                        has2 = true;

                                        assert !has1;

                                        break;

                                    default:
                                        assert false : "Unexpected value in state: " + state[j];
                                }
                            }

                            info(">>>>>>> Checkpoint is fine.");
                        }
                        catch (Throwable e) {
                            error("Failed to load checkpoint: " + e.getMessage());

                            fail.set(true);
                        }
                    }

                    return null;
                }
            },
            1,
            "reader"
        );

        fut1.get();
        fut2.get();
        fut3.get();

        assert !fail.get();
    }

    /**
     * @param val Val to fill with.
     * @return Test array.
     */
    private byte[] createTestArray(byte val) {
        byte[] res = new byte[ARRAY_SIZE];

        Arrays.fill(res, val);

        return res;
    }

    /**
     * @param f Folder to delete.
     */
    void deleteFolder(File f) {
        for (File file : f.listFiles())
            if (file.isDirectory())
                deleteFolder(file);
            else
                file.delete();

        f.delete();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        deleteFolder(new File(U.getIgniteHome(), PATH));
    }
}