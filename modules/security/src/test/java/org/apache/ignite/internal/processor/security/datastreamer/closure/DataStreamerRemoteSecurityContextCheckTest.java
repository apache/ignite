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

package org.apache.ignite.internal.processor.security.datastreamer.closure;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.stream.StreamVisitor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the closure od DataStreamer is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class DataStreamerRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /**
     *
     */
    @Test
    public void testDataStreamer() {
        IgniteEx srvInitiator = grid(SRV_INITIATOR);
        IgniteEx clntInitiator = grid(CLNT_INITIATOR);

        runAndCheck(srvInitiator, () -> dataStreamer(srvInitiator));
        runAndCheck(clntInitiator, () -> dataStreamer(clntInitiator));
    }

    /**
     * @param initiator Initiator node.
     */
    private void dataStreamer(Ignite initiator) {
        try (IgniteDataStreamer<Integer, Integer> strm = initiator.dataStreamer(CACHE_NAME)) {
            strm.receiver(StreamVisitor.from(new TestClosure(grid(SRV_ENDPOINT).localNode().id())));

            strm.addData(prmKey(grid(SRV_TRANSITION)), 100);
        }
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements
        IgniteBiInClosure<IgniteCache<Integer, Integer>, Map.Entry<Integer, Integer>> {
        /** Endpoint node id. */
        private final UUID endpoint;

        /**
         * @param endpoint Endpoint node id.
         */
        public TestClosure(UUID endpoint) {
            this.endpoint = endpoint;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteCache<Integer, Integer> entries,
            Map.Entry<Integer, Integer> entry) {
            IgniteEx loc = (IgniteEx)Ignition.localIgnite();

            VERIFIER.verify(loc);

            //Should check a security context on the endpoint node through compute service
            //because using streamer from receiver may be cause of system worker dead
            loc.compute(loc.cluster().forNodeId(endpoint)).broadcast(new IgniteRunnable() {
                @Override public void run() {
                    VERIFIER.verify(loc);
                }
            });
        }
    }
}
