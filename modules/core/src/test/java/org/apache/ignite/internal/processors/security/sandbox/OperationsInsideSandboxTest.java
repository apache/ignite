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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.FutureAdapter;
import org.apache.ignite.internal.processors.security.impl.TestComputeTask;
import org.apache.ignite.internal.processors.security.impl.TestStoreFactory;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/** . */
public class OperationsInsideSandboxTest extends AbstractSecurityTest {
    /** . */
    private static final String TEST_CACHE = "test_cache";

    /** . */
    protected static final AtomicBoolean IS_EXECUTED = new AtomicBoolean(false);

    /** . */
    @Test
    public void test() throws Exception {
        IgniteEx srvInit = startGrid("srv_init", ALLOW_ALL, false);
        IgniteEx srvRun = startGrid("srv_run", ALLOW_ALL, false);

        srvInit.cluster().active(true);

        srvInit.compute(srvInit.cluster().forNode(srvRun.localNode())).broadcast(() -> {
            CacheConfiguration<String, String> cfg = new CacheConfiguration<String, String>(TEST_CACHE)
                .setCacheStoreFactory(new TestStoreFactory("1", "val"));

            Ignite locNode = Ignition.localIgnite();

            IgniteCache<String, String> cache = locNode.getOrCreateCache(cfg);

            cacheOperations(cache).forEach(this::runOperation);

            cache.destroy();

            computeOperations().forEach(this::runOperation);
        });
    }

    /**
     * @return Stream of cache CRUD operations to test.
     */
    private Stream<GridTestUtils.RunnableX> cacheOperations(IgniteCache<String, String> cache) {
        CacheEntryProcessor<String, String, String> prc = (entry, o) -> {
            IS_EXECUTED.set(true);

            return null;
        };

        IgniteBiPredicate<String, String> p = (a, b) -> {
            IS_EXECUTED.set(true);

            return true;
        };

        Stream<GridTestUtils.RunnableX> crud = Stream.<GridTestUtils.RunnableX>of(
            () -> cache.put("key", "value"),
            () -> cache.putAll(singletonMap("key", "value")),
            () -> cache.get("key"),
            () -> cache.getAll(singleton("key")),
            () -> cache.containsKey("key"),
            () -> cache.remove("key"),
            () -> cache.removeAll(singleton("key")),
            cache::clear,
            () -> cache.replace("key", "value"),
            () -> cache.putIfAbsent("key", "value"),
            () -> cache.getAndPut("key", "value"),
            () -> cache.getAndRemove("key"),
            () -> cache.getAndReplace("key", "value")
        ).map((r) -> new GridTestUtils.RunnableX() {
            @Override public void runx() {
                r.run();

                IS_EXECUTED.set(true);
            }
        });

        return Stream.concat(
            crud,
            Stream.of(
                () -> cache.loadCache(p),
                () -> {
                    cache.put("2", "val_2");

                    cache.query(new ScanQuery<>(p)).getAll();
                },
                () -> cache.invoke("key", prc),
                () -> cache.invokeAll(singleton("key"), prc).get("key"),
                () -> cache.invokeAsync("key", prc).get(),
                () -> cache.invokeAllAsync(singleton("key"), prc).get(),
                () -> {
                    try (IgniteDataStreamer<String, String> strm = Ignition.localIgnite().dataStreamer(TEST_CACHE)) {
                        strm.receiver((k, v) -> IS_EXECUTED.set(true));

                        strm.addData("2", "val");
                    }
                }
            )
        );
    }

    /**
     * @return Stream of Compute and ExecutorService operations to test.
     */
    private Stream<GridTestUtils.RunnableX> computeOperations() {
        Ignite node = Ignition.localIgnite();

        IgniteRunnable runnable = () -> IS_EXECUTED.set(true);

        IgniteCallable<Object> call = () -> {
            IS_EXECUTED.set(true);

            return null;
        };

        IgniteClosure<Object, Object> c = a -> {
            IS_EXECUTED.set(true);

            return null;
        };

        ComputeTask<Object, Object> computeTask = new TestComputeTask(() -> IS_EXECUTED.set(true));

        return Stream.of(
            () -> node.compute().execute(computeTask, 0),
            () -> node.compute().broadcast(call),
            () -> node.compute().call(call),
            () -> node.compute().run(runnable),
            () -> node.compute().apply(c, new Object()),

            () -> new FutureAdapter<>(node.compute().executeAsync(computeTask, 0)).get(),
            () -> new FutureAdapter<>(node.compute().broadcastAsync(call)).get(),
            () -> new FutureAdapter<>(node.compute().callAsync(call)).get(),
            () -> new FutureAdapter<>(node.compute().runAsync(runnable)).get(),
            () -> new FutureAdapter<>(node.compute().applyAsync(c, new Object())).get(),

            () -> node.executorService().invokeAll(singletonList(call)),
            () -> node.executorService().invokeAny(singletonList(call)),
            () -> node.executorService().submit(call).get()
        );
    }

    /** . */
    private void runOperation(GridTestUtils.RunnableX r) {
        IS_EXECUTED.set(false);

        r.run();

        assertTrue(IS_EXECUTED.get());
    }
}
