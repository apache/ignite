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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Checks basic node start/stop operations.
 */
@SuppressWarnings({"CatchGenericClass", "InstanceofCatchParameter"})
@GridCommonTest(group = "Kernal Self")
public class GridStartStopSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int COUNT = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP, GridTestUtils.getNextMulticastGroup(GridStartStopSelfTest.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop() throws Exception {
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
     * TODO: GG-7704
     * @throws Exception If failed.
     */
    public void _testStopWhileInUse() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConnectorConfiguration(null);

        cfg.setGridName(getTestGridName(0));

        CacheConfiguration cc = new CacheConfiguration();

        cc.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cc);

        final Ignite g0 = G.start(cfg);

        cfg = new IgniteConfiguration();

        cfg.setGridName(getTestGridName(1));

        cc = new CacheConfiguration();

        cc.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cc);

        final CountDownLatch latch = new CountDownLatch(1);

        Ignite g1 = G.start(cfg);

        Thread stopper = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    try (Transaction ignored = g0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        g0.cache(null).get(1);

                        latch.countDown();

                        Thread.sleep(500);

                        info("Before stop.");

                        G.stop(getTestGridName(1), true);
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

        g1.cache(null).remove(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoppedState() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConnectorConfiguration(null);

        Ignite ignite = G.start(cfg);

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
    }
}
