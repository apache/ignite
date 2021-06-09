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
package org.apache.ignite.raft.jraft.util.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.ignite.raft.jraft.util.NamedThreadFactory;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.Utils;

/**
 *
 */
public final class DefaultFixedThreadsExecutorGroupFactory implements FixedThreadsExecutorGroupFactory {
    public static final DefaultFixedThreadsExecutorGroupFactory INSTANCE = new DefaultFixedThreadsExecutorGroupFactory();

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
        final int maxPendingTasksPerThread) {
        return newExecutorGroup(nThreads, poolName, maxPendingTasksPerThread, false);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final int nThreads, final String poolName,
        final int maxPendingTasksPerThread, final boolean useMpscQueue) {
        Requires.requireTrue(nThreads > 0, "nThreads must > 0");
        final boolean mpsc = useMpscQueue && Utils.USE_MPSC_SINGLE_THREAD_EXECUTOR;
        final SingleThreadExecutor[] children = new SingleThreadExecutor[nThreads];
        final ThreadFactory threadFactory = mpsc ? new NamedThreadFactory(poolName, true) : null;
        for (int i = 0; i < nThreads; i++) {
            if (mpsc) {
                children[i] = new MpscSingleThreadExecutor(maxPendingTasksPerThread, threadFactory);
            }
            else {
                children[i] = new DefaultSingleThreadExecutor(poolName, maxPendingTasksPerThread);
            }
        }
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children) {
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final SingleThreadExecutor[] children,
        final ExecutorChooserFactory.ExecutorChooser chooser) {
        return new DefaultFixedThreadsExecutorGroup(children, chooser);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children) {
        return new DefaultFixedThreadsExecutorGroup(children);
    }

    @Override
    public FixedThreadsExecutorGroup newExecutorGroup(final ExecutorService[] children,
        final ExecutorChooserFactory.ExecutorChooser chooser) {
        return new DefaultFixedThreadsExecutorGroup(children, chooser);
    }

    private DefaultFixedThreadsExecutorGroupFactory() {
    }
}
