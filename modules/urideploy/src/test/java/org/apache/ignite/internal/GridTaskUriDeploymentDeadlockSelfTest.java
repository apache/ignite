/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Test to reproduce gg-2852.
 */
@RunWith(JUnit4.class)
public class GridTaskUriDeploymentDeadlockSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();

        deploymentSpi.setUriList(
            Arrays.asList(U.resolveIgniteUrl("modules/extdata/uri/target/resources/").toURI().toString()));

        if (igniteInstanceName.endsWith("2")) {
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
    @Test
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

            assert "2".equals(executeAsync(compute(g.cluster().forPredicate(F.equalTo(F.first(g.cluster().forRemotes().nodes())))),
                "GarHelloWorldTask", "HELLOWORLD.MSG").get(60000));

            f.get();
        }
        finally {
            stopAllGrids();
        }
    }
}
