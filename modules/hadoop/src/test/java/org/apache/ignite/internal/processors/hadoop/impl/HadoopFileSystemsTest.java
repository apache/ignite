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

package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopLocalFileSystemV1;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test file systems for the working directory multi-threading support.
 */
public class HadoopFileSystemsTest extends HadoopAbstractSelfTest {
    /** the number of threads */
    private static final int THREAD_COUNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected boolean igfsEnabled() {
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

        cfg.set(HadoopFileSystemsUtils.LOC_FS_WORK_DIR_PROP,
            new Path(new Path(uri), "user/" + System.getProperty("user.name")).toString());

        FileSystem fs = FileSystem.get(uri, cfg);

        assertTrue(fs instanceof HadoopLocalFileSystemV1);

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

            cfg.set(HadoopFileSystemsUtils.LOC_FS_WORK_DIR_PROP, workDir.toString());

            assertEquals(workDir, FileSystem.newInstance(uri, cfg).getWorkingDirectory());

            assertEquals(workDir, newUserInitWorkDir[i]);

            assertEquals(new Path(new Path(uri), "user/user" + i + "/folder" + i), newWorkDir[i]);

            assertEquals(new Path("/folder" + i), newAbsWorkDir[i]);

            assertEquals(new Path(new Path(uri), "user/" + System.getProperty("user.name")), newInstanceWorkDir[i]);
        }

        System.out.println(System.getProperty("user.dir"));
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