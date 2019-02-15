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

package org.apache.ignite.internal.processors.service;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Service serialization test.
 */
@RunWith(JUnit4.class)
public class GridServiceSerializationSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServiceSerialization() throws Exception {
        try {
            Ignite server = startGridsMultiThreaded(3);

            Ignition.setClientMode(true);

            Ignite client = startGrid("client");

            server.services(server.cluster().forServers())
                .deployClusterSingleton("my-service", new MyServiceImpl());

            MyService svc = client.services().serviceProxy("my-service", MyService.class, false);

            svc.hello();

            assert MyServiceImpl.latch.await(2000, TimeUnit.MILLISECONDS);

            assertEquals(0, MyServiceImpl.cnt.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     */
    private static interface MyService extends Service {
        /** */
        void hello();
    }

    /**
     */
    private static class MyServiceImpl implements MyService, Externalizable {
        /** */
        static final AtomicInteger cnt = new AtomicInteger();

        /** */
        static final CountDownLatch latch = new CountDownLatch(1);

        /**
         */
        public MyServiceImpl() throws ClassNotFoundException {
            if (clientThread())
                throw new ClassNotFoundException("Expected ClassNotFoundException");
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            if (clientThread())
                cnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            if (clientThread())
                cnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            if (clientThread())
                cnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void hello() {
            if (clientThread())
                cnt.incrementAndGet();

            latch.countDown();
        }

        /**
         * @return If current thread belongs to client.
         */
        private boolean clientThread() {
            assert Thread.currentThread() instanceof IgniteThread;

            return ((IgniteThread)Thread.currentThread()).getIgniteInstanceName().contains("client");
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }
}
