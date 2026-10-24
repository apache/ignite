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

package org.apache.ignite.internal.benchmarks.jmh.compute;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhNodeStopHook;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.openjdk.jmh.annotations.Benchmark;

/** Compute calls from a client node to the two servers, and a task through the thin client; the jobs do nothing. */
public class JmhComputeAllocationBenchmark extends JmhCacheAbstractBenchmark {
    /** Keys whose primary server runs {@link #affinityCall}. */
    private static final int KEYS = 1024;

    /** Job that does nothing. */
    private static final IgniteRunnable NOOP = new NoopRunnable();

    /** Job that returns a constant. */
    private static final IgniteCallable<Integer> ONE = new OneCallable();

    /** Compute of the client node over the servers. */
    private IgniteCompute compute;

    /** Keys of {@link #affinityCall}. */
    private Integer[] keys;

    /** Thin client, connected to the servers. */
    private IgniteClient thin;

    /** Compute of the thin client over the servers. */
    private ClientCompute thinCompute;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        compute = node.compute(node.cluster().forServers());

        keys = new Integer[KEYS];

        for (int i = 0; i < KEYS; i++)
            keys[i] = i;

        thin = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800", "127.0.0.1:10801"));
        thinCompute = thin.compute(thin.cluster().forServers());
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        // Profilers read the last iteration while the nodes still run.
        JmhNodeStopHook.run();

        thin.close();

        Ignition.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected int dataNodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean clientMode() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration configuration(String igniteInstanceName) {
        ThinClientConfiguration thinCfg = new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(100);

        // The thin client runs compute tasks only if the server allows it.
        return super.configuration(igniteInstanceName)
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setThinClientConfiguration(thinCfg));
    }

    /** Runs a job on one of the servers. */
    @Benchmark
    public void run() {
        compute.run(NOOP);
    }

    /** @return Result of a job run on one of the servers. */
    @Benchmark
    public Integer call() {
        return compute.call(ONE);
    }

    /** @return Result of a job run on the primary server of a key. */
    @Benchmark
    public Integer affinityCall() {
        return compute.affinityCall(cache.getName(), keys[ThreadLocalRandom.current().nextInt(KEYS)], ONE);
    }

    /** @return Result of a task of one job, mapped to the first server. */
    @Benchmark
    public Integer execute() {
        return compute.execute(OneTask.class, null);
    }

    /** Runs a job on each server. */
    @Benchmark
    public void broadcast() {
        compute.broadcast(NOOP);
    }

    /**
     * @return Result of the same task as {@link #execute}, started through the thin client.
     * @throws InterruptedException If interrupted.
     */
    @Benchmark
    public Integer thinExecute() throws InterruptedException {
        return thinCompute.execute(OneTask.class.getName(), null);
    }

    /** Job that does nothing. */
    private static class NoopRunnable implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }

    /** Job that returns 1. */
    private static class OneCallable implements IgniteCallable<Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Integer call() {
            return 1;
        }
    }

    /** Task of one job that returns 1, mapped to the first node of the task's nodes. */
    public static class OneTask extends ComputeTaskAdapter<Object, Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            return Map.of(new OneJob(), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            return results.get(0).getData();
        }
    }

    /** Job of {@link OneTask}. */
    private static class OneJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object execute() {
            return 1;
        }
    }
}
