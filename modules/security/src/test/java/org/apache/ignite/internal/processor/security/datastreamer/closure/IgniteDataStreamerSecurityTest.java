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
import org.apache.ignite.internal.processor.security.AbstractCacheSecurityTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.stream.StreamVisitor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the closure od DataStreamer is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class IgniteDataStreamerSecurityTest extends AbstractCacheSecurityTest {
    /** */
    @Test
    public void testDataStreamer() {
        checkDataStreamer(false);
        checkDataStreamer(true);
    }

    /**
     * @param isTransition True if transition test.
     */
    private void checkDataStreamer(boolean isTransition){
        assertAllowed((t) -> load(clntAllPerms, srvAllPerms, t, isTransition));
        assertAllowed((t) -> load(clntAllPerms, srvReadOnlyPerm, t, isTransition));
        assertAllowed((t) -> load(srvAllPerms, srvAllPerms, t, isTransition));
        assertAllowed((t) -> load(srvAllPerms, srvReadOnlyPerm, t, isTransition));

        assertForbidden((t) -> load(clntReadOnlyPerm, srvAllPerms, t, isTransition));
        assertForbidden((t) -> load(srvReadOnlyPerm, srvAllPerms, t, isTransition));
        assertForbidden((t) -> load(srvReadOnlyPerm, srvReadOnlyPerm, t, isTransition));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void load(IgniteEx initiator, IgniteEx remote, T2<String, Integer> entry, boolean isTransition) {
        try (IgniteDataStreamer<Integer, Integer> strm = initiator.dataStreamer(COMMON_USE_CACHE)) {
            strm.receiver(StreamVisitor.from(closure(remote, entry, isTransition)));

            strm.addData(primaryKey(isTransition ? srvTransitionAllPerms : remote), 100);
        }
    }

    /**
     * @param remote Remote node.
     * @param entry Data to put into test cache.
     * @param isTransition True if transition test.
     * @return Receiver's closure.
     */
    private TestClosure closure(IgniteEx remote, T2<String, Integer> entry, boolean isTransition) {
        if (isTransition)
            return new TransitionTestClosure(
                srvTransitionAllPerms.localNode().id(),
                remote.localNode().id(),
                entry
            );

        return new TestClosure(remote.localNode().id(), entry);
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements
        IgniteBiInClosure<IgniteCache<Integer, Integer>, Map.Entry<Integer, Integer>> {
        /** Remote node id. */
        protected final UUID remoteId;

        /** Data to put into test cache. */
        protected final T2<String, Integer> t2;

        /**
         * @param remoteId Remote node id.
         * @param t2 Data to put into test cache.
         */
        public TestClosure(UUID remoteId, T2<String, Integer> t2) {
            this.remoteId = remoteId;
            this.t2 = t2;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteCache<Integer, Integer> entries,
            Map.Entry<Integer, Integer> entry) {
            Ignite loc = Ignition.localIgnite();

            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(t2.getKey(), t2.getValue());
        }
    }

    /**
     * Closure for transition tests.
     */
    static class TransitionTestClosure extends TestClosure {
        /** Transition node id. */
        private final UUID transitionId;

        /**
         * @param transitionId Transition node id.
         * @param remoteId Remote node id.
         * @param t2 Data to put into test cache.
         */
        public TransitionTestClosure(UUID transitionId, UUID remoteId, T2<String, Integer> t2) {
            super(remoteId, t2);

            this.transitionId = transitionId;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteCache<Integer, Integer> entries,
            Map.Entry<Integer, Integer> entry) {
            Ignite loc = Ignition.localIgnite();

            if (transitionId.equals(loc.cluster().localNode().id())){
                loc.compute(loc.cluster().forNode(loc.cluster().node(remoteId)))
                    .broadcast(()->Ignition.localIgnite().cache(CACHE_NAME).put(t2.getKey(), t2.getValue()));
            }
        }
    }
}
