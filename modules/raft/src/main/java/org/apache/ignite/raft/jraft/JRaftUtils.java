/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.Scheduler;
import org.apache.ignite.raft.jraft.core.TimerManager;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.BootstrapOptions;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.NamedThreadFactory;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.ThreadPoolUtil;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.jraft.util.concurrent.DefaultFixedThreadsExecutorGroupFactory;
import org.apache.ignite.raft.jraft.util.concurrent.FixedThreadsExecutorGroup;

/**
 * Some helper methods for jraft usage.
 */
public final class JRaftUtils {
    /**
     * Bootstrap a non-empty raft node.
     *
     * @param opts options of bootstrap
     * @return true if bootstrap success
     */
    public static boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        final NodeImpl node = new NodeImpl();
        final boolean ret = node.bootstrap(opts);
        node.shutdown();
        node.join();
        return ret;
    }

    /**
     * Create a executor with size.
     *
     * @param prefix thread name prefix
     * @param number thread number
     * @return a new {@link ThreadPoolExecutor} instance
     */
    public static ExecutorService createExecutor(final String prefix, final int number) {
        if (number <= 0) {
            return null;
        }
        return ThreadPoolUtil.newBuilder() //
            .poolName(prefix) //
            .enableMetric(true) //
            .coreThreads(number) //
            .maximumThreads(number) //
            .keepAliveSeconds(60L) //
            .workQueue(new LinkedBlockingQueue<>()) //
            .threadFactory(createThreadFactory(prefix)) //
            .build();
    }

    /**
     * @param opts Node options.
     * @return The executor.
     */
    public static ExecutorService createCommonExecutor(NodeOptions opts) {
        return createExecutor("JRaft-Common-Executor-" + opts.getServerName() + "-", opts.getCommonThreadPollSize());
    }

    /**
     * @param opts Node options.
     * @return The executor.
     */
    public static FixedThreadsExecutorGroup createAppendEntriesExecutor(NodeOptions opts) {
        return createStripedExecutor("JRaft-AppendEntries-Processor-" + opts.getServerName() + "-",
            Utils.APPEND_ENTRIES_THREADS_POOL_SIZE, Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD);
    }

    /**
     * @param opts Node options.
     * @return The executor.
     */
    public static ExecutorService createRequestExecutor(NodeOptions opts) {
        return createExecutor("JRaft-Request-Processor-" + opts.getServerName() + "-",
            opts.getRaftRpcThreadPoolSize());
    }

    /**
     * @param opts Options.
     * @param name The name.
     * @return The service.
     */
    public static ExecutorService createClientExecutor(RpcOptions opts, String name) {
        String prefix = "JRaft-Response-Processor-" + name + "-";
        return ThreadPoolUtil.newBuilder()
            .poolName(prefix) //
            .enableMetric(true) //
            .coreThreads(opts.getRpcProcessorThreadPoolSize() / 3) //
            .maximumThreads(opts.getRpcProcessorThreadPoolSize()) //
            .keepAliveSeconds(60L) //
            .workQueue(new ArrayBlockingQueue<>(10000)) //
            .threadFactory(new NamedThreadFactory(prefix, true)) //
            .build();
    }

    /**
     * @param opts Options.
     * @return The scheduler.
     */
    public static Scheduler createScheduler(NodeOptions opts) {
        return new TimerManager(opts.getTimerPoolSize(), "JRaft-Node-Scheduler-" + opts.getServerName() + "-");
    }

    /**
     * Create a striped executor.
     *
     * @param prefix Thread name prefix.
     * @param number Thread number.
     * @param tasksPerThread Max tasks per thread.
     * @return The executor.
     */
    public static FixedThreadsExecutorGroup createStripedExecutor(final String prefix, final int number,
        final int tasksPerThread) {
        return DefaultFixedThreadsExecutorGroupFactory.INSTANCE
            .newExecutorGroup(
                number,
                prefix,
                tasksPerThread,
                true);
    }

    /**
     * Create a thread factory.
     *
     * @param prefixName the prefix name of thread
     * @return a new {@link ThreadFactory} instance
     */
    public static ThreadFactory createThreadFactory(final String prefixName) {
        return new NamedThreadFactory(prefixName, true);
    }

    /**
     * Create a configuration from a string in the form of "host1:port1[:idx],host2:port2[:idx]......", returns a empty
     * configuration when string is blank.
     */
    public static Configuration getConfiguration(final String s) {
        final Configuration conf = new Configuration();
        if (StringUtils.isBlank(s)) {
            return conf;
        }
        if (conf.parse(s)) {
            return conf;
        }
        throw new IllegalArgumentException("Invalid conf str:" + s);
    }

    /**
     * Create a peer from a string in the form of "host:port[:idx]", returns a empty peer when string is blank.
     */
    public static PeerId getPeerId(final String s) {
        final PeerId peer = new PeerId();
        if (StringUtils.isBlank(s)) {
            return peer;
        }
        if (peer.parse(s)) {
            return peer;
        }
        throw new IllegalArgumentException("Invalid peer str:" + s);
    }

    /**
     * Create a Endpoint instance from  a string in the form of "host:port", returns null when string is blank.
     */
    public static Endpoint getEndPoint(final String s) {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        final String[] tmps = StringUtils.split(s, ':');
        if (tmps.length != 2) {
            throw new IllegalArgumentException("Invalid endpoint string: " + s);
        }
        return new Endpoint(tmps[0], Integer.parseInt(tmps[1]));
    }

    private JRaftUtils() {
    }
}
