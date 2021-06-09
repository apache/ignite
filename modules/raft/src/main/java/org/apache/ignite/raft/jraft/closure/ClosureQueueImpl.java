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
package org.apache.ignite.raft.jraft.closure;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.util.OnlyForTest;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closure queue implementation.
 */
public class ClosureQueueImpl implements ClosureQueue {

    private static final Logger LOG = LoggerFactory.getLogger(ClosureQueueImpl.class);

    private final Lock lock;
    private final NodeOptions options;
    private long firstIndex;
    private LinkedList<Closure> queue;

    @OnlyForTest
    public long getFirstIndex() {
        return firstIndex;
    }

    @OnlyForTest
    public LinkedList<Closure> getQueue() {
        return queue;
    }

    public ClosureQueueImpl(NodeOptions options) {
        super();
        this.lock = new ReentrantLock();
        this.firstIndex = 0;
        this.queue = new LinkedList<>();
        this.options = options;
    }

    @Override
    public void clear() {
        List<Closure> savedQueue;
        this.lock.lock();
        try {
            this.firstIndex = 0;
            savedQueue = this.queue;
            this.queue = new LinkedList<>();
        }
        finally {
            this.lock.unlock();
        }

        final Status status = new Status(RaftError.EPERM, "Leader stepped down");
        Utils.runInThread(options.getCommonExecutor(), () -> {
            for (final Closure done : savedQueue) {
                if (done != null) {
                    done.run(status);
                }
            }
        });
    }

    @Override
    public void resetFirstIndex(final long firstIndex) {
        this.lock.lock();
        try {
            Requires.requireTrue(this.queue.isEmpty(), "Queue is not empty.");
            this.firstIndex = firstIndex;
        }
        finally {
            this.lock.unlock();
        }
    }

    @Override
    public void appendPendingClosure(final Closure closure) {
        this.lock.lock();
        try {
            this.queue.add(closure);
        }
        finally {
            this.lock.unlock();
        }
    }

    @Override
    public long popClosureUntil(final long endIndex, final List<Closure> closures) {
        return popClosureUntil(endIndex, closures, null);
    }

    @Override
    public long popClosureUntil(final long endIndex, final List<Closure> closures,
        final List<TaskClosure> taskClosures) {
        closures.clear();
        if (taskClosures != null) {
            taskClosures.clear();
        }
        this.lock.lock();
        try {
            final int queueSize = this.queue.size();
            if (queueSize == 0 || endIndex < this.firstIndex) {
                return endIndex + 1;
            }
            if (endIndex > this.firstIndex + queueSize - 1) {
                LOG.error("Invalid endIndex={}, firstIndex={}, closureQueueSize={}", endIndex, this.firstIndex,
                    queueSize);
                return -1;
            }
            final long outFirstIndex = this.firstIndex;
            for (long i = outFirstIndex; i <= endIndex; i++) {
                final Closure closure = this.queue.pollFirst();
                if (taskClosures != null && closure instanceof TaskClosure) {
                    taskClosures.add((TaskClosure) closure);
                }
                closures.add(closure);
            }
            this.firstIndex = endIndex + 1;
            return outFirstIndex;
        }
        finally {
            this.lock.unlock();
        }
    }
}
