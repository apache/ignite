/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.service.inner.MyService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for service proxy after client node stopped.
 */
@RunWith(JUnit4.class)
public class GridServiceProxyNodeStopSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProxyHashCode() throws Exception {
        Ignite server = startGrid("server");

        server.services().deployClusterSingleton("my-service", MyServiceFactory.create());

        Ignition.setClientMode(true);

        Ignite client = startGrid("client");

        final MyService proxy = client.services().serviceProxy("my-service", MyService.class, false);

        assertEquals("GridServiceProxy [name=my-service, sticky=false]", proxy.toString());

        assertEquals(42, proxy.hello());
        assertEquals(MyService.HASH, proxy.hashCode(null));

        MyService proxy0 = proxy;

        assertTrue(proxy0.equals(proxy));

        proxy0 = client.services().serviceProxy("my-service", MyService.class, false);

        assertFalse(proxy0.equals(proxy));

        int hash = proxy.hashCode();

        assertFalse(hash == MyService.HASH);

        client.close();

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    proxy.hello();

                    return null;
                }
            },
            IllegalStateException.class,
            null
        );

        int hash0 = proxy.hashCode();

        assertFalse(hash0 == MyService.HASH);

        assertEquals(hash, hash0);
    }
}
