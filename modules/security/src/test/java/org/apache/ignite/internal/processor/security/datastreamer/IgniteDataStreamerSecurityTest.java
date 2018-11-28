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

package org.apache.ignite.internal.processor.security.datastreamer;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheSecurityTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.stream.StreamVisitor;

/**
 * Security tests for IgniteDataStream receiver.
 */
public class IgniteDataStreamerSecurityTest extends AbstractCacheSecurityTest {
    /**
     *
     */
    public void testDataStreamer() {
        assertAllowed(() -> load(clntAllPerms, srvAllPerms, "key"));
        assertAllowed(() -> load(clntAllPerms, srvReadOnlyPerm, "key"));
        assertAllowed(() -> load(srvAllPerms, srvAllPerms, "key"));
        assertAllowed(() -> load(srvAllPerms, srvReadOnlyPerm, "key"));

        assertForbidden(() -> load(clntReadOnlyPerm, srvAllPerms, "fail_key"));
        assertForbidden(() -> load(srvReadOnlyPerm, srvAllPerms, "fail_key"));
        assertForbidden(() -> load(srvReadOnlyPerm, srvReadOnlyPerm, "fail_key"));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     * @param key Key.
     * @return Value that will be to put into cache with passed key.
     */
    private Integer load(IgniteEx initiator, IgniteEx remote, String key) {
        Integer val = values.getAndIncrement();

        try (IgniteDataStreamer<Integer, Integer> strm = initiator.dataStreamer(CACHE_WITHOUT_PERMS)) {
            strm.receiver(
                StreamVisitor.from(
                    new TestClosure(remote.localNode().id(), key, val)
                ));

            strm.addData(primaryKey(remote), 100);
        }

        return val;
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements
        IgniteBiInClosure<IgniteCache<Integer, Integer>, Map.Entry<Integer, Integer>> {
        /** Remote node id. */
        private final UUID remoteId;

        /** Key. */
        private final String key;

        /** Value. */
        private final Integer val;

        /**
         * @param remoteId Remote node id.
         * @param key Key.
         * @param val Value.
         */
        public TestClosure(UUID remoteId, String key, Integer val) {
            this.remoteId = remoteId;
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteCache<Integer, Integer> entries,
            Map.Entry<Integer, Integer> entry) {
            Ignite loc = Ignition.localIgnite();

            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(key, val);
        }
    }
}
