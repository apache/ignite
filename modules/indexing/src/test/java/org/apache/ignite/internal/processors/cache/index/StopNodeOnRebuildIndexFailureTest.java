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

package org.apache.ignite.internal.processors.cache.index;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks correct error handling in index build process.
 */
public class StopNodeOnRebuildIndexFailureTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = StopNodeOnRebuildIndexFailureTest.class.getSimpleName() + "-cache";

    /** Index name. */
    private static final String INDEX_NAME =
        (StopNodeOnRebuildIndexFailureTest.class.getSimpleName() + "_idx").toUpperCase();

    /** Sql table name. */
    private static final String SQL_TABLE = Person.class.getSimpleName();

    /** Create index sql. */
    private static final String CREATE_INDEX_SQL = "CREATE INDEX " + INDEX_NAME + " ON " + SQL_TABLE + " (name)";

    /** */
    private final AtomicBoolean exceptionWasThrown = new AtomicBoolean();

    /** */
    private Supplier<? extends Throwable> errorSupplier;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration<>().setName(CACHE_NAME).setIndexedTypes(Integer.class, Person.class)
        );

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        exceptionWasThrown.set(false);

        BPlusTree.testHndWrapper = (tree, hnd) -> {
            if (tree.getName().toUpperCase().contains(INDEX_NAME)) {
                PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>)hnd;

                return new PageHandler<Object, BPlusTree.Result>() {
                    /** {@inheritDoc} */
                    @Override public BPlusTree.Result run(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        PageIO io,
                        Boolean walPlc,
                        Object arg,
                        int intArg,
                        IoStatisticsHolder statHolder
                    ) throws IgniteCheckedException {
                        try {
                            Throwable t = errorSupplier.get();

                            if (t instanceof Error)
                                throw (Error)t;

                            if (t instanceof RuntimeException)
                                throw (RuntimeException) t;

                            if (t instanceof IgniteCheckedException)
                                throw (IgniteCheckedException) t;
                        }
                        catch (Throwable t) {
                            exceptionWasThrown.set(true);

                            log.error("Exception was thrown ", t);

                            throw t;
                        }

                        return null;
                    }

                    /** {@inheritDoc} */
                    @Override public boolean releaseAfterWrite(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        Object arg,
                        int intArg
                    ) {
                        return delegate.releaseAfterWrite(cacheId, pageId, page, pageAddr, arg, intArg);
                    }
                };
            }

            return hnd;
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        BPlusTree.testHndWrapper = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithRuntimeException() throws Exception {
        startBuildIndexAndThrowExceptionTest(() -> new RuntimeException("Test"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithIgniteCheckedException() throws Exception {
        startBuildIndexAndThrowExceptionTest(() -> new IgniteCheckedException("Test"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithOOMError() throws Exception {
        startBuildIndexAndThrowExceptionTest(() -> new OutOfMemoryError("Test"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithAssertionError() throws Exception {
        startBuildIndexAndThrowExceptionTest(() -> new AssertionError("Test"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithError() throws Exception {
        startBuildIndexAndThrowExceptionTest(() -> new Error("Test"));
    }

    /** */
    private void startBuildIndexAndThrowExceptionTest(Supplier<? extends Throwable> throwableSupplier) throws Exception {
        errorSupplier = throwableSupplier;

        Throwable t = throwableSupplier.get();

        if (t instanceof Exception && !(t instanceof RuntimeException || t instanceof IgniteCheckedException))
            throw new IllegalArgumentException("Invalid throwable class " + t.getClass());

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        assertEquals(1, G.allGrids().size());

        ignite.cache(CACHE_NAME).put(0, new Person(0, "name"));

        //noinspection ThrowableNotThrown
        GridTestUtils.assertThrows(
            log,
            () -> ignite.cache(CACHE_NAME).query(new SqlFieldsQuery(CREATE_INDEX_SQL)).getAll(),
            CacheException.class,
            null
        );

        assertTrue(exceptionWasThrown.get());

        // Node must be stopped by failure handler.
        assertTrue(GridTestUtils.waitForCondition(() -> G.allGrids().isEmpty(), 5_000));
    }
}
