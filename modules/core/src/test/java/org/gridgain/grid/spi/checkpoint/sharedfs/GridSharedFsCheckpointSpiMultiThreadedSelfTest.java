/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.sharedfs;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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

        IgniteFuture fut1 = GridTestUtils.runMultiThreadedAsync(
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

        IgniteFuture fut2 = GridTestUtils.runMultiThreadedAsync(
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

        IgniteFuture fut3 = GridTestUtils.runMultiThreadedAsync(
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

        deleteFolder(new File(U.getGridGainHome(), PATH));
    }
}
