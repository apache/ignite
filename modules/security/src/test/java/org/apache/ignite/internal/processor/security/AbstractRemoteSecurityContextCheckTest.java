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

package org.apache.ignite.internal.processor.security;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
public abstract class AbstractRemoteSecurityContextCheckTest extends AbstractSecurityTest {
    /** Verifier to check results of tests. */
    private static final Verifier VERIFIER = new Verifier();

    /**
     * Checks that current security context is valid and incriments invoke's counter.
     *
     * @param ignite Local node.
     */
    public static void verify(Ignite ignite){
        VERIFIER.verify((IgniteEx)ignite);
    }

    /**
     * @param ign Node.
     * @return Security subject id of passed node.
     */
    protected UUID secSubjectId(IgniteEx ign) {
        return ign.context().security().securityContext().subject().id();
    }

    /**
     * Assert that the passed throwable contains a cause exception with given type.
     *
     * @param throwable Throwable.
     */
    protected void assertCauseSecurityException(Throwable throwable) {
        assertThat(X.cause(throwable, SecurityException.class), notNullValue());
    }

    /**
     * Setups expected behavior to passed verifier.
     */
    protected abstract void setupVerifier(Verifier verifier);

    /**
     * Sets up VERIFIER, performs the runnable and checks the result.
     *
     * @param secSubjId Expected security subject id.
     * @param r Runnable.
     */
    protected final void runAndCheck(UUID secSubjId, Runnable r) {
        setupVerifier(VERIFIER.start(secSubjId));

        r.run();

        VERIFIER.checkResult();
    }

    /**
     * Responsible for verifying of tests results.
     */
    public static class Verifier {
        /**
         * Map that contains an expected behaviour.
         */
        private final ConcurrentHashMap<String, T2<Integer, Integer>> map = new ConcurrentHashMap<>();

        /**
         * Expected security subject id.
         */
        private UUID expSecSubjId;

        /**
         * Prepare for test.
         *
         * @param expSecSubjId Expected security subject id.
         */
        private Verifier start(UUID expSecSubjId) {
            this.expSecSubjId = expSecSubjId;

            map.clear();

            return this;
        }

        /**
         * Adds expected behaivior the method {@link #verify(IgniteEx)} will be invoke exp times on the node with passed
         * name.
         *
         * @param nodeName Node name.
         * @param exp Expected number of invokes.
         */
        public Verifier add(String nodeName, int exp) {
            map.put(nodeName, new T2<>(exp, 0));

            return this;
        }

        /**
         * Checks that current security context is valid and incriments invoke's counter.
         *
         * @param ignite Local node.
         */
        private void verify(IgniteEx ignite) {
            assert expSecSubjId != null;
            assert ignite != null;

            assertThat(
                ignite.context().security().securityContext().subject().id(),
                is(expSecSubjId)
            );

            map.computeIfPresent(ignite.name(),
                new BiFunction<String, T2<Integer, Integer>, T2<Integer, Integer>>() {
                    @Override public T2<Integer, Integer> apply(String name, T2<Integer, Integer> t2) {
                        Integer val = t2.getValue();

                        t2.setValue(++val);

                        return t2;
                    }
                });
        }

        /**
         * Checks result of test and clears expected behavior.
         */
        private void checkResult() {
            assert !map.isEmpty();

            map.forEach((key, value) ->
                assertThat("Node " + key + ". Execution of verify: ",
                    value.get2(), is(value.get1())));

            map.clear();

            expSecSubjId = null;
        }
    }
}