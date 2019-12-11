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
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;

/**
 * Testing operation security context when the ScanQuery, RemoteFilterFactory or TransformerFactory of a
 * ContinuousQueryWithTransformer is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts a ContinuousQueryWithTransformer's component. That
 * component is executed on 'check' node. On every step, it is performed verification that operation securitycontext is
 * the initiator context.
 */
public class ContinuousQueryWithTransformerRemoteSecurityContextCheckTest extends
    AbstractContinuousQueryRemoteSecurityContextCheckTest {
    /** Test InitialQuery of ContinuousQueryWithTransformer. */
    @Test
    public void testInitialQuery() {
        Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> consumer =
            new Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>>() {
                @Override public void accept(ContinuousQueryWithTransformer<Integer, Integer, Integer> q) {
                    q.setInitialQuery(new ScanQuery<>(INITIAL_QUERY_FILTER));
                    q.setRemoteTransformerFactory(new TestTransformerFactory(false));
                }
            };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /** Test RemoteFilterFactory of ContinuousQueryWithTransformer. */
    @Test
    public void testRemoteFilterFactory() {
        Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> consumer =
            new Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>>() {
                @Override public void accept(ContinuousQueryWithTransformer<Integer, Integer, Integer> q) {
                    q.setRemoteFilterFactory(new TestRemoteFilterFactory());
                    q.setRemoteTransformerFactory(new TestTransformerFactory(false));
                }
            };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /** Test TransformerFactory of ContinuousQueryWithTransformer. */
    @Test
    public void testTransformerFactory() {
        Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> consumer =
            new Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>>() {
                @Override public void accept(ContinuousQueryWithTransformer<Integer, Integer, Integer> q) {
                    q.setRemoteTransformerFactory(new TestTransformerFactory(true));
                }
            };

        runAndCheck(grid(SRV_INITIATOR), operation(consumer));
        runAndCheck(grid(CLNT_INITIATOR), operation(consumer));
    }

    /** Test operation. */
    private IgniteRunnable operation(Consumer<ContinuousQueryWithTransformer<Integer, Integer, Integer>> c) {
        return () -> {
            VERIFIER.register();

            ContinuousQueryWithTransformer<Integer, Integer, Integer> cq = new ContinuousQueryWithTransformer<>();

            cq.setLocalListener(e -> {/* No-op. */});

            c.accept(cq);

            openQueryCursor(cq);
        };
    }

    /** Test transformer factory. */
    private static class TestTransformerFactory implements Factory {
        /** True if closure should be registered in verifier. */
        private final boolean register;

        /** Constructor. */
        public TestTransformerFactory(boolean register) {
            this.register = register;
        }

        /** {@inheritDoc} */
        @Override public IgniteClosure<CacheEntryEvent, Object> create() {
            return new TestIgniteClosure(register);
        }
    }

    /** Test IgniteClosure. */
    private static class TestIgniteClosure implements IgniteClosure<CacheEntryEvent, Object> {
        /** True if closure should be registered in verifier. */
        private final boolean register;

        /** Calling of closure should be registered one time only. */
        private final AtomicBoolean executed = new AtomicBoolean(false);

        /** Constructor. */
        public TestIgniteClosure(boolean register) {
            this.register = register;
        }

        /** {@inheritDoc} */
        @Override public Object apply(CacheEntryEvent evt) {
            if (register && !executed.getAndSet(true))
                VERIFIER.register();

            return evt.getValue();
        }
    }
}
