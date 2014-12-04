/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridIpcSharedMemorySpaceSelfTest extends GridCommonAbstractTest {
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

        GridIpcSharedMemoryNativeLoader.load();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBasicOperations() throws Exception {
        File tokFile = new File(GridSystemProperties.getString("java.io.tmpdir"), UUID.randomUUID().toString());

        assert tokFile.createNewFile();

        final String tok = tokFile.getAbsolutePath();

        info("Array length: " + DATA.length);

        final AtomicReference<GridIpcSharedMemorySpace> spaceRef = new AtomicReference<>();

        IgniteFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @SuppressWarnings("TooBroadScope")
                @Override public Object call() throws Exception {
                    try (GridIpcSharedMemorySpace space = new GridIpcSharedMemorySpace(tok, 0, 0, 128, false,
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

        IgniteFuture<?> fut2 = multithreadedAsync(
            new Callable<Object>() {
                @SuppressWarnings({"TooBroadScope", "StatementWithEmptyBody"})
                @Override public Object call() throws Exception {
                    GridIpcSharedMemorySpace inSpace;

                    while ((inSpace = spaceRef.get()) == null) {
                        // No-op;
                    }

                    try (GridIpcSharedMemorySpace space = new GridIpcSharedMemorySpace(tok, 0, 0, 128, true,
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
        File tokFile = new File(GridSystemProperties.getString("java.io.tmpdir"), getTestGridName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        info("Using token file: " + tok);

        Collection<Integer> ids = GridIpcSharedMemoryUtils.sharedMemoryIds();

        info("IDs in the system: " + ids);

        GridIpcSharedMemorySpace space = new GridIpcSharedMemorySpace(tok, GridIpcSharedMemoryUtils.pid(), 0, 128,
            false, log);

        ids = GridIpcSharedMemoryUtils.sharedMemoryIds();

        info("IDs in the system: " + ids);

        assert ids.contains(space.sharedMemoryId());

        // Write some data to the space, but avoid blocking.
        space.write(DATA, 0, 16, 0);

        int shmemId = space.sharedMemoryId();

        space.forceClose();

        ids = GridIpcSharedMemoryUtils.sharedMemoryIds();

        info("IDs in the system: " + ids);

        assert !ids.contains(shmemId);

        assert !tokFile.exists();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadAfterClose() throws Exception {
        File tokFile = new File(GridSystemProperties.getString("java.io.tmpdir"), getTestGridName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        info("Using token file: " + tok);

        GridIpcSharedMemorySpace spaceOut = new GridIpcSharedMemorySpace(tok, GridIpcSharedMemoryUtils.pid(), 0, 128,
            false, log);

        try (GridIpcSharedMemorySpace spaceIn = new GridIpcSharedMemorySpace(tok, GridIpcSharedMemoryUtils.pid(), 0,
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
        File tokFile = new File(GridSystemProperties.getString("java.io.tmpdir"), getTestGridName());

        assert tokFile.createNewFile() || tokFile.exists();

        String tok = tokFile.getAbsolutePath();

        info("Using token file: " + tok);

        try (GridIpcSharedMemorySpace spaceOut = new GridIpcSharedMemorySpace(tok, GridIpcSharedMemoryUtils.pid(),
            GridIpcSharedMemoryUtils.pid(), 128, false, log)) {

            try (GridIpcSharedMemorySpace spaceIn = new GridIpcSharedMemorySpace(tok, GridIpcSharedMemoryUtils.pid(),
                GridIpcSharedMemoryUtils.pid(), 128, true, spaceOut.sharedMemoryId(), log)) {
                // Write some data to the space, but avoid blocking.
                spaceOut.write(DATA, 0, 16, 0);

                spaceIn.close();

                try {
                    spaceOut.write(DATA, 0, 16, 0);

                    assert false;
                }
                catch (GridException e) {
                    info("Caught expected exception: " + e);
                }
            }
        }

        assert !tokFile.exists();
    }
}
