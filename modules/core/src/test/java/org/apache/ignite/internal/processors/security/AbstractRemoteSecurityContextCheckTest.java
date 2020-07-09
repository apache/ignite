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

package org.apache.ignite.internal.processors.security;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;

import static org.apache.ignite.internal.IgnitionEx.localIgnite;

/**
 *
 */
public abstract class AbstractRemoteSecurityContextCheckTest extends AbstractSecurityTest {
    /** Name of server initiator node. */
    protected static final String SRV_INITIATOR = "srv_initiator";

    /** Name of client initiator node. */
    protected static final String CLNT_INITIATOR = "clnt_initiator";

    /** Name of server feature call node. */
    protected static final String SRV_RUN = "srv_run";

    /** Name of client feature call node. */
    protected static final String CLNT_RUN = "clnt_run";

    /** Name of server feature transit node. */
    protected static final String SRV_CHECK = "srv_check";

    /** Name of client feature transit node. */
    protected static final String CLNT_CHECK = "clnt_check";

    /** Name of server endpoint node. */
    protected static final String SRV_ENDPOINT = "srv_endpoint";

    /** Name of client endpoint node. */
    protected static final String CLNT_ENDPOINT = "clnt_endpoint";

    /** Verifier to check results of tests. */
    protected static final Verifier VERIFIER = new Verifier();

    /** Start operation. */
    protected static final String OPERATION_START = "start";

    /** Check operation. */
    protected static final String OPERATION_CHECK = "check";

    /** Endpoint operation. */
    protected static final String OPERATION_ENDPOINT = "endpoint";

    /**
     * @return IgniteCompute is produced by passed node for cluster group that contains nodes with ids from collection.
     */
    protected static IgniteCompute compute(Ignite ignite, Collection<UUID> ids) {
        return ignite.compute(ignite.cluster().forNodeIds(ids));
    }

    /**
     * @return Collection of initiator nodes names.
     */
    protected Collection<IgniteEx> initiators() {
        return Arrays.asList(grid(SRV_INITIATOR), grid(CLNT_INITIATOR));
    }

    /**
     * @return Collection of feature call nodes ids.
     */
    protected final Collection<UUID> nodesToRunIds() {
        return nodesToRun().stream().map(this::nodeId).collect(Collectors.toList());
    }

    /**
     * @return Collection of feature call nodes names.
     */
    protected Collection<String> nodesToRun() {
        return Arrays.asList(SRV_RUN, CLNT_RUN);
    }

    /**
     * @return Collection of feature transit nodes ids.
     */
    protected final Collection<UUID> nodesToCheckIds() {
        return nodesToCheck().stream().map(this::nodeId).collect(Collectors.toList());
    }

    /**
     * @return Collection of feature transit nodes names.
     */
    protected Collection<String> nodesToCheck() {
        return Arrays.asList(SRV_CHECK, CLNT_CHECK);
    }

    /**
     * @return Collection of endpont nodes ids.
     */
    protected final Collection<UUID> endpointIds() {
        return endpoints().stream().map(this::nodeId).collect(Collectors.toList());
    }

    /**
     * @return Collection of endpont nodes names.
     */
    protected Collection<String> endpoints() {
        return Arrays.asList(SRV_ENDPOINT, CLNT_ENDPOINT);
    }

    /**
     * @param name Node name.
     * @return Node id.
     */
    protected UUID nodeId(String name) {
        return grid(name).context().discovery().localNode().id();
    }

    /**
     * Setups expected behavior to passed verifier.
     */
    protected void setupVerifier(final Verifier verifier) {
        Collection<String> toRuns = nodesToRun();

        Collection<String> toChecks = nodesToCheck();

        toRuns.forEach(n -> verifier.expect(n, OPERATION_START, 1));
        toChecks.forEach(n -> verifier.expect(n, OPERATION_CHECK, toRuns.size()));
        endpoints().forEach(n -> verifier.expect(n, OPERATION_ENDPOINT, toChecks.size() * toRuns.size()));
    }

    /**
     * @param op Operation.
     */
    protected void runAndCheck(IgniteRunnable op) {
        runAndCheck(Stream.of(op));
    }

    /**
     * Sets up VERIFIER, performs the runnable and checks the result.
     *
     * @param ops Operations.
     */
    protected void runAndCheck(Stream<IgniteRunnable> ops) {
        ops.forEach(
            r -> initiators().forEach(
                initiator -> {
                    VERIFIER.initiator(initiator);

                    setupVerifier(VERIFIER);

                    compute(initiator, nodesToRunIds())
                        .broadcast((IgniteRunnable)new RegisterExecAndForward<>(OPERATION_START, r, endpointIds()));

                    VERIFIER.checkResult();
                }
            )
        );
    }

    /**
     * Responsible for verifying of tests results.
     */
    public static class Verifier {
        /**
         * Map that contains an expected behaviour.
         */
        private final Map<T2<String, String>, T2<Integer, AtomicInteger>> expInvokes = new ConcurrentHashMap<>();

        /**
         * Checked errors.
         */
        private final Collection<String> errors = new ConcurrentLinkedQueue<>();

        /**
         * Expected security subject id.
         */
        private UUID expSecSubjId;

        /** */
        private Verifier() {
            // No-op.
        }

        /** */
        private void clear() {
            expInvokes.clear();

            errors.clear();

            expSecSubjId = null;
        }

        /**
         * Adds expected behaivior the method {@link #register} will be invoke expected times on the node with passed
         * name and the passed operation name.
         *
         * @param nodeName Node name.
         * @param opName Operation name.
         * @param num Expected number of invokes.
         */
        public Verifier expect(String nodeName, String opName, int num) {
            expInvokes.put(new T2<>(nodeName, opName), new T2<>(num, new AtomicInteger()));

            return this;
        }

        /**
         * Registers a security subject referred for {@code localIgnite} with the passed operation name and increments
         * invoke counter.
         *
         * @param opName Operation name.
         */
        public void register(String opName) {
            if (expSecSubjId == null) {
                error("SubjectId cannot be null.");

                return;
            }

            IgniteEx ignite = localIgnite();

            UUID actualSubjId = secSubjectId(ignite);

            if (!expSecSubjId.equals(actualSubjId)) {
                error("Actual subjectId does not equal expected subjectId " + "[expected=" + expSecSubjId +
                    ", actual=" + actualSubjId + "].");

                return;
            }

            T2<Integer, AtomicInteger> v = expInvokes.get(new T2<>(ignite.name(), opName));

            if (v != null)
                v.get2().incrementAndGet();
            else
                error("Unexpected registration parameters [node=" + ignite.name() + ", opName=" + opName + "].");
        }

        /**
         * Checks result of test and clears expected behavior.
         */
        public void checkResult() {
            if (!errors.isEmpty())
                throw new AssertionError(errors.stream().reduce((s1, s2) -> s1 + "\n" + s2).get());

            expInvokes.forEach((k, v) -> assertEquals("Node \"" + k.get1() + '\"' +
                (k.get2() != null ? ", operation \"" + k.get2() + '\"' : "") +
                ". Execution of register: ", v.get1(), Integer.valueOf(v.get2().get())));
        }

        /** */
        public Verifier initiator(IgniteEx initiator) {
            clear();

            expSecSubjId = secSubjectId(initiator);

            return this;
        }

        /** */
        private UUID secSubjectId(IgniteEx node) {
            return node.context().security().securityContext().subject().id();
        }

        /**
         * @param msg Error message.
         */
        private void error(String msg) {
            errors.add(msg);
        }
    }

    /** */
    protected static class ExecRegisterAndForwardAdapter<K, V> implements IgniteBiInClosure<K, V> {
        /** RegisterExecAndForward. */
        private RegisterExecAndForward<K, V> instance;

        /**
         * @param opName Operation name.
         * @param endpoints Collection of endpont nodes ids.
         */
        public ExecRegisterAndForwardAdapter(String opName, Collection<UUID> endpoints) {
            instance = new RegisterExecAndForward<>(null, opName, endpoints);
        }

        /** {@inheritDoc} */
        @Override public void apply(K k, V v) {
            instance.run();
        }
    }

    /** */
    protected <K, V> RegisterExecAndForward<K, V> operationCheck(String srvName) {
        return new RegisterExecAndForward<>(srvName, OPERATION_CHECK, endpointIds());
    }

    /** */
    protected <K, V> RegisterExecAndForward<K, V> operationCheck() {
        return new RegisterExecAndForward<>(null, OPERATION_CHECK, endpointIds());
    }

    /** */
    protected static class RegisterExecAndForward<K, V> implements IgniteBiPredicate<K, V>, IgniteRunnable,
            IgniteCallable<V>, EntryProcessor<K, V, Object>, IgniteClosure<K, V> {
        /** Runnable. */
        private final IgniteRunnable runnable;

        /** Expected local node name. */
        private final String node;

        /** Operation name. */
        private final String opName;

        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param node Expected local node name.
         * @param opName Operation name.
         * @param endpoints Collection of endpont nodes ids.
         */
        private RegisterExecAndForward(String node, String opName, Collection<UUID> endpoints) {
            this(node, opName, null, endpoints);
        }

        /** */
        private RegisterExecAndForward(String opName, IgniteRunnable runnable, Collection<UUID> endpoints) {
            this(null, opName, runnable, endpoints);
        }

        /** */
        private RegisterExecAndForward(String node, String opName, IgniteRunnable runnable, Collection<UUID> endpoints) {
            this.node = node;
            this.opName = opName;
            this.runnable = runnable;
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(K k, V v) {
            run();

            return false;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            Ignite loc = localIgnite();

            if (node == null || node.equals(loc.name())) {
                VERIFIER.register(opName);

                if (runnable != null)
                    runnable.run();
                else
                    compute(loc, endpoints).broadcast(() -> VERIFIER.register(OPERATION_ENDPOINT));
            }
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<K, V> mutableEntry, Object... objects) {
            run();

            return null;
        }

        /** {@inheritDoc} */
        @Override public V apply(K k) {
            run();

            if (k instanceof Cache.Entry)
                return (V)((Cache.Entry)k).getValue();

            return null;
        }

        /** {@inheritDoc} */
        @Override public V call() {
            run();

            return null;
        }
    }
}
