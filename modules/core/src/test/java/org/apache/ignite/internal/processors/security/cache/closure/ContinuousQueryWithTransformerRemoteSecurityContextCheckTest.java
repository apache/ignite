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

package org.apache.ignite.internal.processors.security.cache.closure;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;

/**
 * Tests check appropriate security context when the scan query, transformer factory}, or remote filter factory of a
 * {@link ContinuousQueryWithTransformer} is executed on a remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts a {@link ContinuousQueryWithTransformer}'s component.
 * That component is executed on 'check' node. On every step, it is performed verification that operation
 * securitycontext is the initiator context.
 */
public class ContinuousQueryWithTransformerRemoteSecurityContextCheckTest extends
    AbstractContinuousQueryRemoteSecurityContextCheckTest {
    /**
     * Tests initial query of {@link ContinuousQueryWithTransformer}.
     */
    @Test
    public void testInitialQuery() {
        Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> consumer =
            new Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>>() {
                @Override public void accept(ContinuousQueryWithTransformer<Integer, Integer, Integer> q) {
                    q.setInitialQuery(new ScanQuery<>(INITIAL_QUERY_FILTER));
                    q.setRemoteTransformerFactory(() -> Cache.Entry::getValue);
                }
            };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /**
     * Tests remote filter factory of {@link ContinuousQueryWithTransformer}.
     */
    @Test
    public void testRemoteFilterFactory() {
        Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> consumer =
            new Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>>() {
                @Override public void accept(ContinuousQueryWithTransformer<Integer, Integer, Integer> q) {
                    q.setRemoteFilterFactory(AbstractContinuousQueryRemoteSecurityContextCheckTest::createRemoteFilter);
                    q.setRemoteTransformerFactory(() -> Cache.Entry::getValue);
                }
            };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /**
     * Tests transformer factory of {@link ContinuousQueryWithTransformer}.
     */
    @Test
    public void testTransformerFactory() {
        Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> consumer =
            new Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>>() {
                @Override public void accept(ContinuousQueryWithTransformer<Integer, Integer, Integer> q) {
                    q.setRemoteTransformerFactory(TestTransformer::new);
                }
            };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /**
     * @param c Consumer that setups a {@link ContinuousQueryWithTransformer}.
     * @return Test operation.
     */
    private IgniteRunnable operation(Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> c) {
        return () -> {
            VERIFIER.register();

            ContinuousQueryWithTransformer<Integer, Integer, Integer> cq = new ContinuousQueryWithTransformer<>();

            cq.setLocalListener(e -> {/* No-op. */});

            c.accept(cq);

            openQueryCursor(cq);
        };
    }

    /** Test Transformer. */
    static class TestTransformer implements
        IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Integer>, Integer> {
        /** Should be registred one time only. */
        private AtomicBoolean registred = new AtomicBoolean(false);

        /** {@inheritDoc} */
        @Override public Integer apply(CacheEntryEvent<? extends Integer, ? extends Integer> evt) {
            if (!registred.getAndSet(true))
                VERIFIER.register();

            return evt.getValue();
        }
    }
}
