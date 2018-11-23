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

package org.apache.ignite.internal.processor.security.cache;

import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security test for scan query.
 */
public class ScanQuerySecurityTest extends AbstractCacheSecurityTest {
    /** */
    public void testScanQuery() throws Exception {
        putTestData(srvAllPerms, CACHE_NAME);
        putTestData(srvAllPerms, CACHE_WITHOUT_PERMS);

        awaitPartitionMapExchange();

        successQuery(clntAllPerms, srvAllPerms, CACHE_NAME);
        successQuery(srvAllPerms, srvAllPerms, CACHE_NAME);
        successQuery(clntAllPerms, srvAllPerms, CACHE_WITHOUT_PERMS);
        successQuery(srvAllPerms, srvAllPerms, CACHE_WITHOUT_PERMS);

        successTransform(clntAllPerms, srvAllPerms, CACHE_NAME);
        successTransform(srvAllPerms, srvAllPerms, CACHE_NAME);
        successTransform(clntAllPerms, srvAllPerms, CACHE_WITHOUT_PERMS);
        successTransform(srvAllPerms, srvAllPerms, CACHE_WITHOUT_PERMS);

        successQuery(clntAllPerms, srvReadOnlyPerm, CACHE_NAME);
        successQuery(srvAllPerms, srvReadOnlyPerm, CACHE_NAME);
        successQuery(clntAllPerms, srvReadOnlyPerm, CACHE_WITHOUT_PERMS);
        successQuery(srvAllPerms, srvReadOnlyPerm, CACHE_WITHOUT_PERMS);

        successTransform(clntAllPerms, srvReadOnlyPerm, CACHE_NAME);
        successTransform(srvAllPerms, srvReadOnlyPerm, CACHE_NAME);
        successTransform(clntAllPerms, srvReadOnlyPerm, CACHE_WITHOUT_PERMS);
        successTransform(srvAllPerms, srvReadOnlyPerm, CACHE_WITHOUT_PERMS);

        failQuery(clntReadOnlyPerm, srvAllPerms, CACHE_NAME);
        failQuery(srvReadOnlyPerm, srvAllPerms, CACHE_NAME);
        failQuery(clntReadOnlyPerm, srvAllPerms, CACHE_WITHOUT_PERMS);
        failQuery(srvReadOnlyPerm, srvAllPerms, CACHE_WITHOUT_PERMS);

        failTransform(clntReadOnlyPerm, srvAllPerms, CACHE_NAME);
        failTransform(srvReadOnlyPerm, srvAllPerms, CACHE_NAME);
        failTransform(clntReadOnlyPerm, srvAllPerms, CACHE_WITHOUT_PERMS);
        failTransform(srvReadOnlyPerm, srvAllPerms, CACHE_WITHOUT_PERMS);
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void failQuery(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        assertCause(
            GridTestUtils.assertThrowsWithCause(
                () -> {
                    initiator.cache(cacheName).query(
                        new ScanQuery<>(
                            new QueryFilter(remote.localNode().id(), "fail_key", -1)
                        )
                    ).getAll();
                }
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void successQuery(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        Integer val = values.getAndIncrement();

        initiator.cache(cacheName).query(
            new ScanQuery<>(
                new QueryFilter(remote.localNode().id(), "key", val)
            )
        ).getAll();

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void failTransform(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        assertCause(
            GridTestUtils.assertThrowsWithCause(
                () -> {
                    initiator.cache(cacheName).query(
                        new ScanQuery<>((k, v) -> true),
                        new Transformer(remote.localNode().id(), "fail_key", -1)
                    ).getAll();
                }
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void successTransform(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        Integer val = values.getAndIncrement();

        initiator.cache(cacheName).query(
            new ScanQuery<>((k, v) -> true),
            new Transformer(remote.localNode().id(), "key", val)
        ).getAll();

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
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
     * Common class for test closures.
     */
    static class CommonClosure {
        /** Remote node id. */
        protected final UUID remoteId;

        /** Key. */
        private final String key;

        /** Value. */
        private final Integer val;

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public CommonClosure(UUID remoteId, String key, Integer val) {
            this.remoteId = remoteId;
            this.key = key;
            this.val = val;
        }

        /**
         * Put value to cache.
         */
        protected void put() {
            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(key, val);
        }
    }

    /**
     * Test query filter.
     * */
    static class QueryFilter extends CommonClosure implements IgniteBiPredicate<String, Integer> {
        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public QueryFilter(UUID remoteId, String key, Integer val) {
            super(remoteId, key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(String s, Integer i) {
            put();

            return false;
        }
    }

    /**
     * Test transformer.
     */
    static class Transformer extends CommonClosure implements IgniteClosure<Cache.Entry<String, Integer>, Integer> {
        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public Transformer(UUID remoteId, String key, Integer val) {
            super(remoteId, key, val);
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<String, Integer> entry) {
            put();

            return entry.getValue();
        }
    }
}
