/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class IgniteWalFormatFileFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE = "testCache";

    /** */
    private static final String formatFile = "formatFile";

    /** Fail method name reference. */
    private final AtomicReference<String> failMtdNameRef = new AtomicReference<>();

    /** */
    private boolean fsync;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration(TEST_CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(2048L * 1024 * 1024)
                .setPersistenceEnabled(true))
            .setWalMode(fsync ? WALMode.FSYNC : WALMode.BACKGROUND)
            .setWalBufferSize(1024 * 1024)
            .setWalSegmentSize(512 * 1024)
            .setFileIOFactory(new FailingFileIOFactory(failMtdNameRef));

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setFailureHandler(new TestFailureHandler(false));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStartFailedFsync() throws Exception {
        fsync = true;

        failMtdNameRef.set(formatFile);

        checkCause(GridTestUtils.assertThrows(log, () -> startGrid(0), IgniteCheckedException.class, null));
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10035")
    @Test
    public void testFailureHandlerTriggeredFsync() throws Exception {
        fsync = true;

        failFormatFileOnClusterActivate();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10035")
    @Test
    public void testFailureHandlerTriggered() throws Exception {
        fsync = false;

        failFormatFileOnClusterActivate();
    }

    /**
     * @throws Exception If failed.
     */
    private void failFormatFileOnClusterActivate() throws Exception {
        failMtdNameRef.set(null);

        startGrid(0);
        startGrid(1);

        if (!fsync) {
            setFileIOFactory(grid(0).context().cache().context().wal());
            setFileIOFactory(grid(1).context().cache().context().wal());
        }

        failMtdNameRef.set(formatFile);

        grid(0).cluster().active(true);

        checkCause(failureHandler(0).awaitFailure(2000).error());
        checkCause(failureHandler(1).awaitFailure(2000).error());
    }

    /**
     * @param mtdName Method name.
     */
    private static boolean isCalledFrom(String mtdName) {
        return isCalledFrom(Thread.currentThread().getStackTrace(), mtdName);
    }

    /**
     * @param stackTrace Stack trace.
     * @param mtdName Method name.
     */
    private static boolean isCalledFrom(StackTraceElement[] stackTrace, String mtdName) {
        return Arrays.stream(stackTrace).map(StackTraceElement::getMethodName).anyMatch(mtdName::equals);
    }

    /**
     * @param gridIdx Grid index.
     * @return Failure handler configured for grid with given index.
     */
    private TestFailureHandler failureHandler(int gridIdx) {
        FailureHandler hnd = grid(gridIdx).configuration().getFailureHandler();

        assertTrue(hnd instanceof TestFailureHandler);

        return (TestFailureHandler)hnd;
    }

    /**
     * @param t Throwable.
     */
    private void checkCause(Throwable t) {
        StorageException e = X.cause(t, StorageException.class);

        assertNotNull(e);
        assertNotNull(e.getMessage());
        assertTrue(e.getMessage().contains("Failed to format WAL segment file"));

        IOException ioe = X.cause(e, IOException.class);

        assertNotNull(ioe);
        assertNotNull(ioe.getMessage());
        assertTrue(ioe.getMessage().contains("No space left on device"));

        assertTrue(isCalledFrom(ioe.getStackTrace(), formatFile));
    }

    /** */
    private void setFileIOFactory(IgniteWriteAheadLogManager wal) {
        if (wal instanceof FileWriteAheadLogManager)
            ((FileWriteAheadLogManager)wal).setFileIOFactory(new FailingFileIOFactory(failMtdNameRef));
        else
            fail(wal.getClass().toString());
    }

    /**
     * Create File I/O which fails if specific method call present in stack trace.
     */
    private static class FailingFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** Fail method name reference. */
        private final AtomicReference<String> failMtdNameRef;

        /**
         * @param failMtdNameRef Fail method name reference.
         */
        FailingFileIOFactory(AtomicReference<String> failMtdNameRef) {
            assertNotNull(failMtdNameRef);

            this.failMtdNameRef = failMtdNameRef;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            final FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {
                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    conditionalFail();

                    return super.write(buf, off, len);
                }

                @Override public void clear() throws IOException {
                    conditionalFail();

                    super.clear();
                }

                private void conditionalFail() throws IOException {
                    String failMtdName = failMtdNameRef.get();

                    if (failMtdName != null && isCalledFrom(failMtdName))
                        throw new IOException("No space left on device");
                }
            };
        }
    }
}
