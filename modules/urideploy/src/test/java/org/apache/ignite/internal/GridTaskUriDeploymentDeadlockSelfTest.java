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
import org.apache.ignite.events.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.deployment.uri.*;
import org.apache.ignite.testframework.junits.common.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;

/**
 * Test to reproduce gg-2852.
 */
public class GridTaskUriDeploymentDeadlockSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();

        deploymentSpi.setUriList(
            Arrays.asList(U.resolveGridGainUrl("modules/core/src/test/resources/").toURI().toString()));

        if (gridName.endsWith("2")) {
            // Delay deployment for 2nd grid only.
            Field f = deploymentSpi.getClass().getDeclaredField("delayOnNewOrUpdatedFile");

            f.setAccessible(true);

            f.set(deploymentSpi, true);
        }

        c.setDeploymentSpi(deploymentSpi);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeadlock() throws Exception {
        try {
            Ignite g = startGrid(1);

            final CountDownLatch latch = new CountDownLatch(1);

            g.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt.type() == EVT_NODE_JOINED;

                    latch.countDown();

                    return true;
                }
            }, EVT_NODE_JOINED);

            IgniteInternalFuture<?> f = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(2);

                    return null;
                }
            }, 1);

            assert latch.await(5, SECONDS);

            info(">>> Starting task.");

            executeAsync(compute(g.cluster().forPredicate(F.equalTo(F.first(g.cluster().forRemotes().nodes())))),
                "GridGarHelloWorldTask", "HELLOWORLD.MSG").get(60000);

            f.get();
        }
        catch (Exception e) {
            error("Test failed.", e);

            // With former version of GridDeploymentLocalStore test hangs forever.
            // So, we need to forcibly exit.
            // System.exit(1);
        }
        finally {
            stopAllGrids();
        }
    }
}
