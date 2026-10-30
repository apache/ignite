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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhNodeStopHook;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

/** Compute calls to the two servers from a client node or a server, and a task through the thin client. */
public class JmhComputeAllocationBenchmark extends JmhCacheAbstractBenchmark {
    /** Keys whose primary server runs {@link #affinityCall}. */
    private static final int KEYS = 1024;

    /** Node that calls compute. */
    public enum Client {
        /** A server node. */
        SERVER,

        /** A client node. */
        THICK
    }

    /** Node that calls compute. */
    @Param("THICK")
    private Client client;

    /** Length of the {@code byte[]} in each job's argument and result, 0 for none. */
    @Param("0")
    private int argSize;

    /** Argument of the jobs and the task, {@code null} with {@code argSize=0}. */
    private byte[] arg;

    /** Job that holds {@link #arg} and does nothing. */
    private IgniteRunnable runJob;

    /** Job that returns {@link #arg}. */
    private IgniteCallable<byte[]> callJob;

    /** Compute of the calling node over the servers. */
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

        arg = argSize == 0 ? null : new byte[argSize];
        runJob = new ArgRunnable(arg);
        callJob = new EchoCallable(arg);

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
        return client == Client.THICK;
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
        compute.run(runJob);
    }

    /** @return Result of a job run on one of the servers. */
    @Benchmark
    public byte[] call() {
        return compute.call(callJob);
    }

    /** @return Result of a job run on the primary server of a key. */
    @Benchmark
    public byte[] affinityCall() {
        return compute.affinityCall(cache.getName(), keys[ThreadLocalRandom.current().nextInt(KEYS)], callJob);
    }

    /** @return Result of a task of one job. */
    @Benchmark
    public byte[] execute() {
        return compute.execute(EchoTask.class, arg);
    }

    /** Runs a job on each server. */
    @Benchmark
    public void broadcast() {
        compute.broadcast(runJob);
    }

    /**
     * @return Result of the same task as {@link #execute}, started through the thin client.
     * @throws InterruptedException If interrupted.
     */
    @Benchmark
    public byte[] thinExecute() throws InterruptedException {
        return thinCompute.execute(EchoTask.class.getName(), arg);
    }

    /** Job that holds an argument and does nothing. */
    private static class ArgRunnable implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Argument. */
        private final byte[] arg;

        /** @param arg Argument. */
        ArgRunnable(byte[] arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }

    /** Job that returns its argument. */
    private static class EchoCallable implements IgniteCallable<byte[]> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Argument. */
        private final byte[] arg;

        /** @param arg Argument. */
        EchoCallable(byte[] arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public byte[] call() {
            return arg;
        }
    }

    /** Task of one job that returns the task's argument; the load balancer picks the job's node. */
    public static class EchoTask extends ComputeTaskSplitAdapter<byte[], byte[]> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, byte[] arg) {
            return List.of(new EchoJob(arg));
        }

        /** {@inheritDoc} */
        @Override public byte[] reduce(List<ComputeJobResult> results) {
            return results.get(0).getData();
        }
    }

    /** Job of {@link EchoTask}. */
    private static class EchoJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Argument. */
        private final byte[] arg;

        /** @param arg Argument. */
        EchoJob(byte[] arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            return arg;
        }
    }
}
