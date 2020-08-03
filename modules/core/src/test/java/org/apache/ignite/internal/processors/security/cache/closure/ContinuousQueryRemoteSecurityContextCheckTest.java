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
 * Tests check appropriate security context when the scan query, remote filter, or
 * remote filter factory of a {@link ContinuousQuery} is executed on a remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts a {@link ContinuousQuery}'s component. That component
 * is executed on 'check' node. On every step, it is performed verification that operation securitycontext is the
 * initiator context.
 */
public class ContinuousQueryRemoteSecurityContextCheckTest extends
    AbstractContinuousQueryRemoteSecurityContextCheckTest {
    /**
     * Test initial query of {@link ContinuousQuery}.
     */
    @Test
    public void testInitialQuery() {
        Consumer<ContinuousQuery<Integer, Integer>> consumer = new Consumer<ContinuousQuery<Integer, Integer>>() {
            @Override public void accept(ContinuousQuery<Integer, Integer> q) {
                q.setInitialQuery(new ScanQuery<>(INITIAL_QUERY_FILTER));
                q.setLocalListener(e -> {/* No-op. */});
            }
        };

        runAndCheck(operation(consumer, true));
    }

    /**
     * Tests remote filter factory of {@link ContinuousQuery}.
     */
    @Test
    public void testRemoteFilterFactory() {
        Consumer<ContinuousQuery<Integer, Integer>> consumer = new Consumer<ContinuousQuery<Integer, Integer>>() {
            @Override public void accept(ContinuousQuery<Integer, Integer> q) {
                q.setRemoteFilterFactory(() -> RMT_FILTER);
            }
        };

        runAndCheck(operation(consumer));
    }

    /**
     * Tests remote filter of {@link ContinuousQuery}.
     */
    @Test
    public void testRemoteFilter() {
        Consumer<ContinuousQuery<Integer, Integer>> consumer = new Consumer<ContinuousQuery<Integer, Integer>>() {
            @Override public void accept(ContinuousQuery<Integer, Integer> q) {
                q.setRemoteFilter(RMT_FILTER);
            }
        };

        runAndCheck(operation(consumer));
    }

    /**
     * @param c Consumer that setups a {@link ContinuousQuery}.
     * @param init True if needing put data to a cache before openning a cursor.
     * @return Test operation.
     */
    private IgniteRunnable operation(Consumer<ContinuousQuery<Integer, Integer>> c, boolean init) {
        return () -> {
            VERIFIER.register(OPERATION_OPEN_CQ);

            ContinuousQuery<Integer, Integer> cq = new ContinuousQuery<>();

            c.accept(cq);

            executeQuery(cq, init);
        };
    }

    /**
     * @param c Consumer that setups a {@link ContinuousQuery}.
     * @return Test operation.
     */
    private IgniteRunnable operation(Consumer<ContinuousQuery<Integer, Integer>> c) {
       return operation(c, false);
    }
}
