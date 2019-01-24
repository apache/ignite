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

package org.apache.ignite.internal.processor.security.cache.closure;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheSecurityTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the filter of ScanQuery is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class ScanQuerySecurityTest extends AbstractCacheSecurityTest {
    /** */
    @Test
    public void testScanQuery() throws Exception {
        putTestData(srvAllPerms, CACHE_NAME);
        putTestData(srvAllPerms, COMMON_USE_CACHE);

        awaitPartitionMapExchange();

        assertAllowed((t) -> query(clntAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> query(srvAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> query(clntAllPerms, srvAllPerms, COMMON_USE_CACHE, t));
        assertAllowed((t) -> query(srvAllPerms, srvAllPerms, COMMON_USE_CACHE, t));

        assertAllowed((t) -> transform(clntAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> transform(srvAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> transform(clntAllPerms, srvAllPerms, COMMON_USE_CACHE, t));
        assertAllowed((t) -> transform(srvAllPerms, srvAllPerms, COMMON_USE_CACHE, t));

        assertAllowed((t) -> query(clntAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> query(srvAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> query(clntAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));
        assertAllowed((t) -> query(srvAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));

        assertAllowed((t) -> transform(clntAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> transform(srvAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> transform(clntAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));
        assertAllowed((t) -> transform(srvAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));

        assertAllowed((t) -> transitionQuery(clntAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> transitionQuery(srvAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> transitionQuery(clntAllPerms, srvAllPerms, COMMON_USE_CACHE, t));
        assertAllowed((t) -> transitionQuery(srvAllPerms, srvAllPerms, COMMON_USE_CACHE, t));

        assertAllowed((t) -> transitionTransform(clntAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> transitionTransform(srvAllPerms, srvAllPerms, CACHE_NAME, t));
        assertAllowed((t) -> transitionTransform(clntAllPerms, srvAllPerms, COMMON_USE_CACHE, t));
        assertAllowed((t) -> transitionTransform(srvAllPerms, srvAllPerms, COMMON_USE_CACHE, t));

        assertAllowed((t) -> transitionQuery(clntAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> transitionQuery(srvAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> transitionQuery(clntAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));
        assertAllowed((t) -> transitionQuery(srvAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));

        assertAllowed((t) -> transitionTransform(clntAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> transitionTransform(srvAllPerms, srvReadOnlyPerm, CACHE_NAME, t));
        assertAllowed((t) -> transitionTransform(clntAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));
        assertAllowed((t) -> transitionTransform(srvAllPerms, srvReadOnlyPerm, COMMON_USE_CACHE, t));

        assertForbidden((t) -> query(clntReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> query(srvReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> query(clntReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));
        assertForbidden((t) -> query(srvReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));

        assertForbidden((t) -> transform(clntReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> transform(srvReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> transform(clntReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));
        assertForbidden((t) -> transform(srvReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));

        assertForbidden((t) -> transitionQuery(clntReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> transitionQuery(srvReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> transitionQuery(clntReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));
        assertForbidden((t) -> transitionQuery(srvReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));

        assertForbidden((t) -> transitionTransform(clntReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> transitionTransform(srvReadOnlyPerm, srvAllPerms, CACHE_NAME, t));
        assertForbidden((t) -> transitionTransform(clntReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));
        assertForbidden((t) -> transitionTransform(srvReadOnlyPerm, srvAllPerms, COMMON_USE_CACHE, t));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void query(IgniteEx initiator, IgniteEx remote, String cacheName, T2<String, Integer> entry) {
        assert !remote.localNode().isClient();

        initiator.cache(cacheName).query(
            new ScanQuery<>(
                new QueryFilter(remote.localNode().id(), entry)
            )
        ).getAll();
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void transitionQuery(IgniteEx initiator, IgniteEx remote, String cacheName, T2<String, Integer> entry) {
        assert !remote.localNode().isClient();

        initiator.cache(cacheName).query(
            new ScanQuery<>(
                new TransitionQueryFilter(srvTransitionAllPerms.localNode().id(),
                    remote.localNode().id(), cacheName, entry)
            )
        ).getAll();
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void transform(IgniteEx initiator, IgniteEx remote, String cacheName, T2<String, Integer> entry) {
        assert !remote.localNode().isClient();

        initiator.cache(cacheName).query(
            new ScanQuery<>(new TransformerFilter(remote.localNode().id())),
            new Transformer(entry)
        ).getAll();
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     */
    private void transitionTransform(IgniteEx initiator, IgniteEx remote, String cacheName, T2<String, Integer> entry) {
        assert !remote.localNode().isClient();

        initiator.cache(cacheName).query(
            new ScanQuery<>(new TransformerFilter(srvTransitionAllPerms.localNode().id())),
            new TransitionTransformer(remote.localNode().id(), cacheName, entry)
        ).getAll();
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void putTestData(IgniteEx ignite, String cacheName) {
        try (IgniteDataStreamer<String, Integer> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 1; i <= 100; i++)
                streamer.addData(Integer.toString(i), i);
        }
    }

    /**
     * Test query filter.
     */
    static class QueryFilter implements IgniteBiPredicate<String, Integer> {
        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /** Remote node id. */
        protected UUID remoteId;

        /** Data to put into test cache. */
        protected T2<String, Integer> t2;

        /**
         * @param remoteId Remote node id.
         * @param t2 Data to put into test cache.
         */
        public QueryFilter(UUID remoteId, T2<String, Integer> t2) {
            this.remoteId = remoteId;
            this.t2 = t2;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(String s, Integer i) {
            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(t2.getKey(), t2.getValue());

            return false;
        }
    }

    /** */
    static class TransitionQueryFilter extends QueryFilter {
        /** Transition id. */
        private final UUID transitionId;

        /** Cache name. */
        private final String cacheName;

        /**
         * @param transitionId Transition id.
         * @param remoteId Remote id.
         * @param cacheName Cache name.
         * @param t2 Data to put into test cache.
         */
        public TransitionQueryFilter(UUID transitionId, UUID remoteId,
            String cacheName, T2<String, Integer> t2) {
            super(remoteId, t2);

            this.transitionId = transitionId;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(String s, Integer i) {
            if (transitionId.equals(loc.cluster().localNode().id())) {
                loc.cache(cacheName).query(
                    new ScanQuery<>(
                        new QueryFilter(remoteId, t2)
                    )
                ).getAll();
            }

            return false;
        }
    }

    /**
     * Test transformer.
     */
    static class Transformer implements IgniteClosure<Cache.Entry<String, Integer>, Integer> {
        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /** Data to put into test cache. */
        protected final T2<String, Integer> t2;

        /**
         * @param t2 Data to put into test cache.
         */
        public Transformer(T2<String, Integer> t2) {
            this.t2 = t2;
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<String, Integer> entry) {
            loc.cache(CACHE_NAME).put(t2.getKey(), t2.getValue());

            return entry.getValue();
        }
    }

    /** */
    static class TransitionTransformer extends Transformer {
        /** Remote node id. */
        private final UUID remoteId;

        /** Cache name. */
        private final String cacheName;

        /**
         * @param remoteId Remote id.
         * @param cacheName Cache name.
         * @param t2 Data to put into test cache.
         */
        public TransitionTransformer(UUID remoteId, String cacheName, T2<String, Integer> t2) {
            super(t2);

            this.remoteId = remoteId;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<String, Integer> entry) {
            loc.cache(cacheName).query(
                new ScanQuery<>(new TransformerFilter(remoteId)),
                new Transformer(t2)
            ).getAll();

            return entry.getValue();
        }
    }

    /** */
    static class TransformerFilter implements IgniteBiPredicate<String, Integer> {
        /** Node id. */
        private final UUID nodeId;

        /** Filter must return true only one time. */
        private AtomicBoolean b = new AtomicBoolean(true);

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param nodeId Node id.
         */
        public TransformerFilter(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(String s, Integer integer) {
            return nodeId.equals(loc.cluster().localNode().id()) && b.getAndSet(false);
        }
    }
}
