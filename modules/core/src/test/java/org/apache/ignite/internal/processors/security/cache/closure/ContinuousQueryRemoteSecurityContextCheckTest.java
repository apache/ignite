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

import java.util.function.Consumer;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;

/**
 * Tests check appropriate security context when the {@code ScanQuery}, {@code RemoteFilter}, or {@code
 * RemoteFilterFactory} of a {@code ContinuousQuery} is executed on a remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts a {@code ContinuousQuery}'s component. That component
 * is executed on 'check' node. On every step, it is performed verification that operation securitycontext is the
 * initiator context.
 */
public class ContinuousQueryRemoteSecurityContextCheckTest extends
    AbstractContinuousQueryRemoteSecurityContextCheckTest {
    /**
     * Test {@code InitialQuery} of {@code ContinuousQuery}.
     */
    @Test
    public void testInitialQuery() {
        Consumer<ContinuousQuery<Integer, Integer>> consumer = new Consumer<ContinuousQuery<Integer, Integer>>() {
            @Override public void accept(ContinuousQuery<Integer, Integer> q) {
                q.setInitialQuery(new ScanQuery<>(INITIAL_QUERY_FILTER));
            }
        };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /**
     * Tests {@code RemoteFilterFactory} of {@code ContinuousQuery}.
     */
    @Test
    public void testRemoteFilterFactory() {
        Consumer<ContinuousQuery<Integer, Integer>> consumer = new Consumer<ContinuousQuery<Integer, Integer>>() {
            @Override public void accept(ContinuousQuery<Integer, Integer> q) {
                q.setRemoteFilterFactory(new TestRemoteFilterFactory());
            }
        };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /**
     * Tests {@code RemoteFilter} of {@code ContinuousQuery}.
     */
    @Test
    public void testRemoteFilter() {
        Consumer<ContinuousQuery<Integer, Integer>> consumer = new Consumer<ContinuousQuery<Integer, Integer>>() {
            @Override public void accept(ContinuousQuery<Integer, Integer> q) {
                q.setRemoteFilter(new TestCacheEntryEventFilter());
            }
        };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /**
     * @param c {@code Consumer} that setups a {@code ContinuousQuery}.
     * @return Test operation.
     */
    private IgniteRunnable operation(Consumer<ContinuousQuery<Integer, Integer>> c) {
        return () -> {
            VERIFIER.register();

            ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();

            cq.setLocalListener(e -> {/* No-op. */});

            c.accept(cq);

            openQueryCursor(cq);
        };
    }
}
