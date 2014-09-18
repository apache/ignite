/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.kernal.processors.hadoop.fs.*;
import org.gridgain.testframework.GridTestUtils;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test file systems for the working directory multi-threading support.
 */
public class GridHadoopFileSystemsTest extends GridHadoopAbstractSelfTest {
    private static final int THREAD_COUNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected boolean ggfsEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }


    /**
     * Test the file system with specified URI for the multi-thread working directory support.
     *
     * @param uri Base URI of the file system (scheme and authority).
     * @throws Exception If fails.
     */
    private void testFileSystem(final URI uri) throws Exception {
        final Configuration cfg = new Configuration();

        setupFileSystems(cfg);

        cfg.set(GridHadoopFileSystemsUtils.LOC_FS_WORK_DIR_PROP,
            new Path(new Path(uri), "user/" + System.getProperty("user.name")).toString());

        final CountDownLatch changeUserPhase = new CountDownLatch(THREAD_COUNT);
        final CountDownLatch changeDirPhase = new CountDownLatch(THREAD_COUNT);
        final CountDownLatch changeAbsDirPhase = new CountDownLatch(THREAD_COUNT);
        final CountDownLatch finishPhase = new CountDownLatch(THREAD_COUNT);

        final Path[] newUserInitWorkDir = new Path[THREAD_COUNT];
        final Path[] newWorkDir = new Path[THREAD_COUNT];
        final Path[] newAbsWorkDir = new Path[THREAD_COUNT];
        final Path[] newInstanceWorkDir = new Path[THREAD_COUNT];

        final AtomicInteger threadNum = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    int curThreadNum = threadNum.getAndIncrement();

                    FileSystem fs = FileSystem.get(uri, cfg);

                    GridHadoopFileSystemsUtils.setUser(fs, "user" + curThreadNum);

                    if ("file".equals(uri.getScheme()))
                        FileSystem.get(uri, cfg).setWorkingDirectory(new Path("file:///user/user" + curThreadNum));

                    changeUserPhase.countDown();
                    changeUserPhase.await();

                    newUserInitWorkDir[curThreadNum] = FileSystem.get(uri, cfg).getWorkingDirectory();

                    FileSystem.get(uri, cfg).setWorkingDirectory(new Path("folder" + curThreadNum));

                    changeDirPhase.countDown();
                    changeDirPhase.await();

                    newWorkDir[curThreadNum] = FileSystem.get(uri, cfg).getWorkingDirectory();

                    FileSystem.get(uri, cfg).setWorkingDirectory(new Path("/folder" + curThreadNum));

                    changeAbsDirPhase.countDown();
                    changeAbsDirPhase.await();

                    newAbsWorkDir[curThreadNum] = FileSystem.get(uri, cfg).getWorkingDirectory();

                    newInstanceWorkDir[curThreadNum] = FileSystem.newInstance(uri, cfg).getWorkingDirectory();

                    finishPhase.countDown();
                }
                catch (InterruptedException | IOException e) {
                    error("Failed to execute test thread.", e);

                    fail();
                }
            }
        }, THREAD_COUNT, "filesystems-test");

        finishPhase.await();

        for (int i = 0; i < THREAD_COUNT; i ++) {
            cfg.set(MRJobConfig.USER_NAME, "user" + i);

            Path workDir = new Path(new Path(uri), "user/user" + i);

            cfg.set(GridHadoopFileSystemsUtils.LOC_FS_WORK_DIR_PROP, workDir.toString());

            assertEquals(workDir, FileSystem.newInstance(uri, cfg).getWorkingDirectory());

            assertEquals(workDir, newUserInitWorkDir[i]);

            assertEquals(new Path(new Path(uri), "user/user" + i + "/folder" + i), newWorkDir[i]);

            assertEquals(new Path("/folder" + i), newAbsWorkDir[i]);

            assertEquals(new Path(new Path(uri), "user/" + System.getProperty("user.name")), newInstanceWorkDir[i]);
        }

        System.out.println(System.getProperty("user.dir"));
    }

    /**
     * Test GGFS multi-thread working directory.
     *
     * @throws Exception If fails.
     */
    public void testGgfs() throws Exception {
        testFileSystem(URI.create(ggfsScheme()));
    }

    /**
     * Test HDFS multi-thread working directory.
     *
     * @throws Exception If fails.
     */
    public void testHdfs() throws Exception {
        testFileSystem(URI.create("hdfs://localhost/"));
    }

    /**
     * Test LocalFS multi-thread working directory.
     *
     * @throws Exception If fails.
     */
    public void testLocal() throws Exception {
        testFileSystem(URI.create("file:///"));
    }
}
