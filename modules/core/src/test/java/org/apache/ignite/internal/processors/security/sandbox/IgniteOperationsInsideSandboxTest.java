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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * A user-defined code inside the sandbox can use the public API of Ignite without additional
 * sandbox permissions.
 */
public class IgniteOperationsInsideSandboxTest extends AbstractSandboxTest {
    /** Test compute task. */
    private static final ComputeTask<Object, Object> TEST_COMPUTE_TASK = new ComputeTask<Object, Object>() {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            return Collections.singletonMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        // No-op.
                    }

                    @Override public Object execute() {
                        return null;
                    }
                }, subgrid.stream().findFirst().orElseThrow(IllegalStateException::new)
            );
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    };

    /** Test callable. */
    private static final IgniteCallable<Object> TEST_CALLABLE = new IgniteCallable<Object>() {
        @Override public Object call() {
            return null;
        }
    };

    /** Test runnable. */
    private static final IgniteRunnable TEST_RUNNABLE = new IgniteRunnable() {
        @Override public void run() {
            //No-op.
        }
    };

    /** Test closure. */
    private static final IgniteClosure<Object, Object> TEST_CLOSURE = new IgniteClosure<Object, Object>() {
        @Override public Object apply(Object o) {
            return null;
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration<String, String>(TEST_CACHE)
                    .setCacheStoreFactory(new TestStoreFactory("1", "val"))
            );
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        Ignite srv = startGrid(SRV, ALLOW_ALL, false);

        Ignite clnt = startGrid(CLNT_ALLOWED_THREAD_START, ALLOW_ALL, true);

        srv.cluster().active(true);

        testCacheOperations(clnt);

        testComputeOperations(clnt);

        testDataStreamerOperations(clnt);
    }

    /**
     *
     */
    private void testComputeOperations(Ignite clnt) {
        clnt.compute(clnt.cluster().forRemotes()).broadcast(
            () -> {
                Ignite node = Ignition.localIgnite();

                node.compute().execute(TEST_COMPUTE_TASK, 0);
                node.compute().broadcast(TEST_CALLABLE);
                node.compute().call(TEST_CALLABLE);
                node.compute().run(TEST_RUNNABLE);
                node.compute().apply(TEST_CLOSURE, new Object());
                node.compute().executeAsync(TEST_COMPUTE_TASK, 0).get();
                node.compute().broadcastAsync(TEST_CALLABLE).get();
                node.compute().callAsync(TEST_CALLABLE).get();
                node.compute().runAsync(TEST_RUNNABLE).get();
                node.compute().applyAsync(TEST_CLOSURE, new Object()).get();
                try {
                    node.executorService().invokeAll(singletonList(TEST_CALLABLE));
                    node.executorService().invokeAny(singletonList(TEST_CALLABLE));
                    node.executorService().submit(TEST_CALLABLE).get();
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new IgniteException(e);
                }
            }
        );
    }

    /**
     *
     */
    private void testCacheOperations(Ignite clnt) {
        clnt.compute(clnt.cluster().forRemotes()).broadcast(
            () -> {
                IgniteCache<String, String> cache = Ignition.localIgnite().cache(TEST_CACHE);

                cache.put("key", "val");
                cache.putAll(singletonMap("key", "value"));
                cache.get("key");
                cache.getAll(Collections.singleton("key"));
                cache.containsKey("key");
                cache.remove("key");
                cache.removeAll(Collections.singleton("key"));
                cache.clear();
                cache.replace("key", "value");
                cache.putIfAbsent("key", "value");
                cache.getAndPut("key", "value");
                cache.getAndRemove("key");
                cache.getAndReplace("key", "value");

                cache.invoke("key", processor());
                cache.invokeAll(singleton("key"), processor());
                cache.invokeAsync("key", processor()).get();
                cache.invokeAllAsync(singleton("key"), processor()).get();

                cache.query(new ScanQuery<String, Integer>()).getAll();
            }
        );
    }

    /** */
    private void testDataStreamerOperations(Ignite clnt) {
        clnt.compute(clnt.cluster().forRemotes())
            .broadcast(() -> {
                try (IgniteDataStreamer<String, String> s = Ignition.localIgnite().dataStreamer(TEST_CACHE)) {
                    s.addData("k", "val");
                    s.addData(singletonMap("key", "val"));
                    s.addData((Map.Entry<String, String>)entry());
                    s.addData(singletonList(entry()));
                }
            });
    }

    /**
     *
     */
    private CacheEntryProcessor<String, String, String> processor() {
        return (entry, o) -> {
            entry.setValue("Val");

            return null;
        };
    }

    /**
     * @return Cache entry for test.
     */
    protected T2<String, String> entry() {
        return new T2<>("key", "val");
    }
}
