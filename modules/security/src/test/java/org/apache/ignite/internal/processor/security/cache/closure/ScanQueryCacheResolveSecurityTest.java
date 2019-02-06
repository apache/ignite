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

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheResolveSecurityTest;
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
public class ScanQueryCacheResolveSecurityTest extends AbstractCacheResolveSecurityTest {
    /**
     *
     */
    @Test
    public void test() throws Exception {
        IgniteEx srvInitiator = grid("srv_initiator");

        IgniteEx clntInitiator = grid("clnt_initiator");

        IgniteEx srvTransition = grid("srv_transition");

        IgniteEx srvEndpoint = grid("srv_endpoint");

        srvInitiator.cache(CACHE_NAME).put(prmKey(srvTransition), 1);
        srvInitiator.cache(CACHE_NAME).put(prmKey(srvEndpoint), 2);

        awaitPartitionMapExchange();

        perform(srvInitiator, () -> query(srvInitiator));
        perform(clntInitiator, () -> query(clntInitiator));

        perform(srvInitiator, () -> transform(srvInitiator));
        perform(clntInitiator, () -> transform(clntInitiator));
    }

    /**
     * @param initiator Initiator node.
     */
    private void query(IgniteEx initiator) {
        initiator.cache(CACHE_NAME).query(
            new ScanQuery<>(
                new QueryFilter("srv_transition", "srv_endpoint")
            )
        ).getAll();
    }

    /**
     * @param initiator Initiator node.
     */
    private void transform(IgniteEx initiator) {
        initiator.cache(CACHE_NAME).query(
            new ScanQuery<>((k, v) -> true),
            new Transformer("srv_transition", "srv_endpoint")
        ).getAll();
    }

    /**
     * Test query filter.
     */
    static class QueryFilter implements IgniteBiPredicate<Integer, Integer> {
        /** Locale ignite. */
        @IgniteInstanceResource
        private Ignite loc;

        /** Expected local node name. */
        private final String node;

        /** Endpoint node name. */
        private final String endpoint;

        /**
         * @param node Expected local node name.
         * @param endpoint Endpoint node name.
         */
        public QueryFilter(String node, String endpoint) {
            this.node = node;
            this.endpoint = endpoint;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer s, Integer i) {
            if (node.equals(loc.name())) {
                VERIFIER.verify(loc);

                if (endpoint != null) {
                    loc.cache(CACHE_NAME).query(
                        new ScanQuery<>(new QueryFilter(endpoint, null))
                    ).getAll();
                }
            }

            return false;
        }
    }

    /**
     * Test transformer.
     */
    static class Transformer implements IgniteClosure<Cache.Entry<Integer, Integer>, Integer> {
        /** Locale ignite. */
        @IgniteInstanceResource
        private Ignite loc;

        /** Expected local node name. */
        private final String node;

        /** Endpoint node name. */
        private final String endpoint;

        /**
         * @param node Expected local node name.
         * @param endpoint Endpoint node name.
         */
        public Transformer(String node, String endpoint) {
            this.node = node;
            this.endpoint = endpoint;
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
            if (node.equals(loc.name())) {
                VERIFIER.verify(loc);

                if (endpoint != null) {
                    loc.cache(CACHE_NAME).query(
                        new ScanQuery<>((k, v) -> true),
                        new Transformer(endpoint, null)
                    ).getAll();
                }
            }

            return entry.getValue();
        }
    }
}
