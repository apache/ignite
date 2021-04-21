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
import java.util.stream.Stream;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.junit.Test;

import static java.util.Collections.singleton;

/**
 * Checks that user-defined code for cache operations is executed inside the sandbox.
 */
public class CacheSandboxTest extends AbstractSandboxTest {
    /** */
    private static final CacheEntryProcessor<Object, Object, Object> TEST_PROC = (entry, o) -> {
        controlAction();

        return null;
    };

    /** */
    private static final IgniteBiPredicate<String, String> TEST_PRED = (a, b) -> {
        controlAction();

        return true;
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<String, String>(TEST_CACHE)
                    .setCacheStoreFactory(new TestStoreFactory("1", "val"))
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        populateCache();
    }

    /** */
    @Test
    public void testEntryProcessor() {
        entryProcessorOperations(grid(CLNT_ALLOWED_WRITE_PROP)).forEach(this::runOperation);
        entryProcessorOperations(grid(CLNT_FORBIDDEN_WRITE_PROP))
            .forEach(r -> runForbiddenOperation(r, AccessControlException.class));
    }

    /** */
    @Test
    public void testScanQuery() {
        scanQueryOperations(grid(CLNT_ALLOWED_WRITE_PROP)).forEach(this::runOperation);
        scanQueryOperations(grid(CLNT_FORBIDDEN_WRITE_PROP))
            .forEach(r -> runForbiddenOperation(r, AccessControlException.class));
    }

    /** */
    @Test
    public void testLoadCache() {
        runOperation(() -> grid(CLNT_ALLOWED_WRITE_PROP).<String, String>cache(TEST_CACHE).loadCache(TEST_PRED));
        runForbiddenOperation(() -> grid(CLNT_FORBIDDEN_WRITE_PROP)
            .<String, String>cache(TEST_CACHE).loadCache(TEST_PRED), AccessControlException.class);
    }

    /**
     * @return EntryProcessor operations to test.
     */
    private Stream<RunnableX> entryProcessorOperations(Ignite node) {
        EntryProcessorResult<Object> dflt = () -> null;

        return Stream.of(
            () -> node.cache(TEST_CACHE).invoke("key", TEST_PROC),
            () -> node.cache(TEST_CACHE).invokeAll(singleton("key"), TEST_PROC)
                .getOrDefault("key", dflt).get(),
            () -> node.cache(TEST_CACHE).invokeAsync("key", TEST_PROC).get(),
            () -> node.cache(TEST_CACHE).invokeAllAsync(singleton("key"), TEST_PROC).get()
                .getOrDefault("key", dflt).get()
        );
    }

    /**
     * @return ScanQuery operations to test.
     */
    private Stream<RunnableX> scanQueryOperations(Ignite node) {
        return Stream.of(
            () -> node.cache(TEST_CACHE).query(new ScanQuery<>((o, o2) -> {
                controlAction();

                return false;
            })).getAll(),
            () -> node.cache(TEST_CACHE).query(new ScanQuery<>((k, v) -> true), e -> {
                controlAction();

                return null;
            }).getAll()
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
