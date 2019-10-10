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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Collections.singleton;

/**
 * Checks that user-defined code for cache operations is executed inside the sandbox.
 */
public class CacheSandboxTest extends AbstractSandboxTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<String, String>(TEST_CACHE)
                    .setCacheStoreFactory(new TestStoreFactory("1", "val"))
            );
    }

    /** */
    @Test
    public void test() throws Exception {
        prepareCluster();
        populateCache();

        Ignite clntAllowed = grid(CLNT_ALLOWED_THREAD_START);

        Ignite clntFrobidden = grid(CLNT_FORBIDDEN_THREAD_START);

        entryProcessorOperations(clntAllowed).forEach(this::runOperation);
        entryProcessorOperations(clntFrobidden).forEach(this::runForbiddenOperation);

        scanQueryOperations(clntAllowed).forEach(this::runOperation);
        scanQueryOperations(clntFrobidden).forEach(r -> runForbiddenOperation(r, AccessControlException.class));

        runOperation(loadCacheOperation(clntAllowed));
        runForbiddenOperation(loadCacheOperation(clntFrobidden), AccessControlException.class);
    }

    /**
     * @return EntryProcessor operations to test.
     */
    private Stream<Supplier<Object>> entryProcessorOperations(Ignite node) {
        EntryProcessorResult<Object> dflt = new EntryProcessorResult<Object>() {
            @Override public Object get() throws EntryProcessorException {
                return null;
            }
        };

        return Stream.of(
            () -> node.cache(TEST_CACHE).invoke("key", processor()),
            () -> node.cache(TEST_CACHE).invokeAll(singleton("key"), processor())
                .getOrDefault("key", dflt).get(),
            () -> node.cache(TEST_CACHE).invokeAsync("key", processor()).get(),
            () -> node.cache(TEST_CACHE).invokeAllAsync(singleton("key"), processor()).get()
                .getOrDefault("key", dflt).get()
        );
    }

    /** */
    private CacheEntryProcessor<Object, Object, Object> processor() {
        return (entry, o) -> {
            START_THREAD_RUNNABLE.run();

            return null;
        };
    }

    /**
     * @return ScanQuery operations to test.
     */
    private Stream<GridTestUtils.RunnableX> scanQueryOperations(Ignite node) {
        return Stream.of(
            () -> node.cache(TEST_CACHE).query(
                new ScanQuery<>(new IgniteBiPredicate<Object, Object>() {
                    @Override public boolean apply(Object o, Object o2) {
                        START_THREAD_RUNNABLE.run();

                        return false;
                    }
                })
            ).getAll(),
            () -> node.cache(TEST_CACHE).query(
                new ScanQuery<>((k, v) -> true),
                new IgniteClosure<Cache.Entry<Object, Object>, Object>() {
                    @Override public Object apply(Cache.Entry<Object, Object> entry) {
                        START_THREAD_RUNNABLE.run();

                        return null;
                    }
                }
            ).getAll()
        );
    }

    /**
     * @return LoadCache operation to test.
     */
    private GridTestUtils.RunnableX loadCacheOperation(Ignite node) {
        return () -> node.<String, String>cache(TEST_CACHE).loadCache(
            (a, b) -> {
                START_THREAD_RUNNABLE.run();

                return true;
            }
        );
    }

    /** */
    private void populateCache() {
        try (IgniteDataStreamer<String, Integer> cache = grid(SRV).dataStreamer(TEST_CACHE)) {
            for (int i = 1; i <= 10; i++)
                cache.addData(Integer.toString(i), i);
        }
    }
}
