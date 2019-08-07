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

package org.apache.ignite.internal;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Checks basic node start/stop operations.
 */
@SuppressWarnings({"InstanceofCatchParameter"})
@GridCommonTest(group = "Kernal Self")
public class GridStartStopSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int COUNT = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP, GridTestUtils.getNextMulticastGroup(GridStartStopSelfTest.class));
    }

    /**
     */
    @Test
    public void testStartStop() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConnectorConfiguration(null);

        info("Grid start-stop test count: " + COUNT);

        for (int i = 0; i < COUNT; i++) {
            info("Starting grid.");

            try (Ignite g = G.start(cfg)) {
                assert g != null;

                info("Stopping grid " + g.cluster().localNode().id());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopWhileInUse() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConnectorConfiguration(null);

        cfg.setIgniteInstanceName(getTestIgniteInstanceName(0));

        CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cc.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cc);

        final Ignite g0 = G.start(cfg);

        cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(getTestIgniteInstanceName(1));

        cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cc.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cc);

        final CountDownLatch latch = new CountDownLatch(1);

        Ignite g1 = G.start(cfg);

        Thread stopper = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    try (Transaction ignored = g0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        g0.cache(DEFAULT_CACHE_NAME).get(1);

                        latch.countDown();

                        Thread.sleep(500);

                        info("Before stop.");

                        G.stop(getTestIgniteInstanceName(1), true);
                    }
                }
                catch (Exception e) {
                    error("Error.", e);
                }
            }
        });

        stopper.start();

        assert latch.await(1, SECONDS);

        info("Before remove.");

        try {
            g1.cache(DEFAULT_CACHE_NAME).remove(1);
        }
        catch (CacheException ignore) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStoppedState() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConnectorConfiguration(null);

        IgniteEx ignite = startGrid(cfg);

        assert ignite != null;

        G.stop(ignite.name(), true);

        try {
            ignite.cluster().localNode();
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }

        try {
            ignite.cluster().nodes();

            assert false;
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }

        try {
            ignite.cluster().forRemotes();

            assert false;
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }

        try {
            ignite.compute().localTasks();

            assert false;
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }

        //check all executors are terminated
        GridKernalContext ctx = ignite.context();

        Map<String, ExecutorService> executors =
            Arrays.stream(GridKernalContext.class.getMethods())
                .filter(method -> method.getReturnType().equals(ExecutorService.class))
                .collect(
                    HashMap::new,
                    (map, method) -> {
                        try {
                            String mtdName = method.getName();
                            String executorSvcKey = mtdName.startsWith("get") ? mtdName.substring(3) : mtdName;
                            map.put(executorSvcKey, (ExecutorService)method.invoke(ctx));
                        }
                        catch (IllegalAccessException | InvocationTargetException e) {
                            throw new IgniteException(e);
                        }
                    },
                    HashMap::putAll
                );

        String errs = executors.entrySet().stream()
            .filter(e -> !(e.getValue() == null || e.getValue().isTerminated()))
            .map(e -> e.getKey() + " not terminated.")
            .collect(Collectors.joining("\n"));

        assertTrue(errs, errs == null || errs.isEmpty());
    }
}
