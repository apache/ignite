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
package org.apache.ignite.raft.jraft.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.FSMCaller.LastAppliedLogIndexListener;
import org.apache.ignite.raft.jraft.ReadOnlyService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.entity.ReadIndexState;
import org.apache.ignite.raft.jraft.entity.ReadIndexStatus;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.error.RaftException;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.ReadOnlyServiceOptions;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexResponse;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosureAdapter;
import org.apache.ignite.raft.jraft.util.ByteString;
import org.apache.ignite.raft.jraft.util.Bytes;
import org.apache.ignite.raft.jraft.util.DisruptorBuilder;
import org.apache.ignite.raft.jraft.util.DisruptorMetricSet;
import org.apache.ignite.raft.jraft.util.LogExceptionHandler;
import org.apache.ignite.raft.jraft.util.NamedThreadFactory;
import org.apache.ignite.raft.jraft.util.OnlyForTest;
import org.apache.ignite.raft.jraft.util.ThreadHelper;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only service implementation.
 */
public class ReadOnlyServiceImpl implements ReadOnlyService, LastAppliedLogIndexListener {
    private static final int MAX_ADD_REQUEST_RETRY_TIMES = 3;

    /**
     * Disruptor to run readonly service.
     */
    private Disruptor<ReadIndexEvent> readIndexDisruptor;

    private RingBuffer<ReadIndexEvent> readIndexQueue;

    private RaftOptions raftOptions;

    private NodeImpl node;

    private final Lock lock = new ReentrantLock();

    private FSMCaller fsmCaller;

    private volatile CountDownLatch shutdownLatch;

    private NodeMetrics nodeMetrics;

    private volatile RaftException error;

    // <logIndex, statusList>
    private final TreeMap<Long, List<ReadIndexStatus>> pendingNotifyStatus = new TreeMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(ReadOnlyServiceImpl.class);

    private static class ReadIndexEvent {
        Bytes requestContext;
        ReadIndexClosure done;
        CountDownLatch shutdownLatch;
        long startTime;
    }

    private static class ReadIndexEventFactory implements EventFactory<ReadIndexEvent> {
        @Override
        public ReadIndexEvent newInstance() {
            return new ReadIndexEvent();
        }
    }

    private class ReadIndexEventHandler implements EventHandler<ReadIndexEvent> {
        // task list for batch
        private final List<ReadIndexEvent> events = new ArrayList<>(
            ReadOnlyServiceImpl.this.raftOptions.getApplyBatch());

        @Override
        public void onEvent(final ReadIndexEvent newEvent, final long sequence, final boolean endOfBatch)
            throws Exception {
            if (newEvent.shutdownLatch != null) {
                executeReadIndexEvents(this.events);
                this.events.clear();
                newEvent.shutdownLatch.countDown();
                return;
            }

            this.events.add(newEvent);
            if (this.events.size() >= ReadOnlyServiceImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
                executeReadIndexEvents(this.events);
                this.events.clear();
            }
        }
    }

    /**
     * ReadIndexResponse process closure
     */
    class ReadIndexResponseClosure extends RpcResponseClosureAdapter<ReadIndexResponse> {

        final List<ReadIndexState> states;
        final ReadIndexRequest request;

        ReadIndexResponseClosure(final List<ReadIndexState> states, final ReadIndexRequest request) {
            super();
            this.states = states;
            this.request = request;
        }

        /**
         * Called when ReadIndex response returns.
         */
        @Override
        public void run(final Status status) {
            if (!status.isOk()) {
                notifyFail(status);
                return;
            }
            final ReadIndexResponse readIndexResponse = getResponse();
            if (!readIndexResponse.getSuccess()) {
                notifyFail(new Status(-1, "Fail to run ReadIndex task, maybe the leader stepped down."));
                return;
            }
            // Success
            final ReadIndexStatus readIndexStatus = new ReadIndexStatus(this.states, this.request,
                readIndexResponse.getIndex());
            for (final ReadIndexState state : this.states) {
                // Records current commit log index.
                state.setIndex(readIndexResponse.getIndex());
            }

            boolean doUnlock = true;
            ReadOnlyServiceImpl.this.lock.lock();
            try {
                if (readIndexStatus.isApplied(ReadOnlyServiceImpl.this.fsmCaller.getLastAppliedIndex())) {
                    // Already applied, notify readIndex request.
                    ReadOnlyServiceImpl.this.lock.unlock();
                    doUnlock = false;
                    notifySuccess(readIndexStatus);
                }
                else {
                    // Not applied, add it to pending-notify cache.
                    ReadOnlyServiceImpl.this.pendingNotifyStatus
                        .computeIfAbsent(readIndexStatus.getIndex(), k -> new ArrayList<>(10)) //
                        .add(readIndexStatus);
                }
            }
            finally {
                if (doUnlock) {
                    ReadOnlyServiceImpl.this.lock.unlock();
                }
            }
        }

        private void notifyFail(final Status status) {
            final long nowMs = Utils.monotonicMs();
            for (final ReadIndexState state : this.states) {
                ReadOnlyServiceImpl.this.nodeMetrics.recordLatency("read-index", nowMs - state.getStartTimeMs());
                final ReadIndexClosure done = state.getDone();
                if (done != null) {
                    final Bytes reqCtx = state.getRequestContext();
                    done.run(status, ReadIndexClosure.INVALID_LOG_INDEX, reqCtx != null ? reqCtx.get() : null);
                }
            }
        }
    }

    private void executeReadIndexEvents(final List<ReadIndexEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        final ReadIndexRequest.Builder rb = ReadIndexRequest.newBuilder() //
            .setGroupId(this.node.getGroupId()) //
            .setServerId(this.node.getServerId().toString());

        final List<ReadIndexState> states = new ArrayList<>(events.size());

        for (final ReadIndexEvent event : events) {
            byte[] ctx = event.requestContext.get();
            rb.addEntries(ctx == null ? ByteString.EMPTY : new ByteString(ctx));
            states.add(new ReadIndexState(event.requestContext, event.done, event.startTime));
        }
        final ReadIndexRequest request = rb.build();

        this.node.handleReadIndexRequest(request, new ReadIndexResponseClosure(states, request));
    }

    private void resetPendingStatusError(final Status st) {
        this.lock.lock();
        try {
            for (final List<ReadIndexStatus> statuses : this.pendingNotifyStatus.values()) {
                for (final ReadIndexStatus status : statuses) {
                    reportError(status, st);
                }
            }
            this.pendingNotifyStatus.clear();
        }
        finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean init(final ReadOnlyServiceOptions opts) {
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        this.fsmCaller = opts.getFsmCaller();
        this.raftOptions = opts.getRaftOptions();

        this.readIndexDisruptor = DisruptorBuilder.<ReadIndexEvent>newInstance() //
            .setEventFactory(new ReadIndexEventFactory()) //
            .setRingBufferSize(this.raftOptions.getDisruptorBufferSize()) //
            .setThreadFactory(new NamedThreadFactory("JRaft-ReadOnlyService-Disruptor-" +
                node.getOptions().getServerName() + "-" + node.getNodeId().toString(), true)) //
            .setWaitStrategy(new BlockingWaitStrategy()) //
            .setProducerType(ProducerType.MULTI) //
            .build();
        this.readIndexDisruptor.handleEventsWith(new ReadIndexEventHandler());
        this.readIndexDisruptor
            .setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.readIndexQueue = this.readIndexDisruptor.start();
        if (this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry() //
                .register("jraft-read-only-service-disruptor", new DisruptorMetricSet(this.readIndexQueue));
        }
        // listen on lastAppliedLogIndex change events.
        this.fsmCaller.addLastAppliedLogIndexListener(this);

        // start scanner.
        this.node.getTimerManager().scheduleAtFixedRate(() -> onApplied(this.fsmCaller.getLastAppliedIndex()),
            this.raftOptions.getMaxElectionDelayMs(), this.raftOptions.getMaxElectionDelayMs(), TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public synchronized void setError(final RaftException error) {
        if (this.error == null) {
            this.error = error;
        }
    }

    @Override
    public synchronized void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        this.shutdownLatch = new CountDownLatch(1);
        Utils.runInThread(this.node.getOptions().getCommonExecutor(),
            () -> this.readIndexQueue.publishEvent((event, sequence) -> event.shutdownLatch = this.shutdownLatch));
    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
        }
        this.readIndexDisruptor.shutdown();
        resetPendingStatusError(new Status(RaftError.ESTOP, "Node is quit."));
    }

    @Override
    public void addRequest(final byte[] reqCtx, final ReadIndexClosure closure) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), closure, new Status(RaftError.EHOSTDOWN, "Was stopped"));
            throw new IllegalStateException("Service already shutdown.");
        }

        try {
            EventTranslator<ReadIndexEvent> translator = (event, sequence) -> {
                event.done = closure;
                event.requestContext = new Bytes(reqCtx);
                event.startTime = Utils.monotonicMs();
            };
            int retryTimes = 0;
            while (true) {
                if (this.readIndexQueue.tryPublishEvent(translator)) {
                    break;
                }
                else {
                    retryTimes++;
                    if (retryTimes > MAX_ADD_REQUEST_RETRY_TIMES) {
                        Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), closure,
                            new Status(RaftError.EBUSY, "Node is busy, has too many read-only requests."));
                        this.nodeMetrics.recordTimes("read-index-overload-times", 1);
                        LOG.warn("Node {} ReadOnlyServiceImpl readIndexQueue is overload.", this.node.getNodeId());
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }
        }
        catch (final Exception e) {
            Utils.runClosureInThread(this.node.getOptions().getCommonExecutor(), closure, new Status(RaftError.EPERM, "Node is down."));
        }
    }

    /**
     * Called when lastAppliedIndex updates.
     *
     * @param appliedIndex applied index
     */
    @Override
    public void onApplied(final long appliedIndex) {
        // TODO reuse pendingStatuses list? https://issues.apache.org/jira/browse/IGNITE-14832
        List<ReadIndexStatus> pendingStatuses = null;
        this.lock.lock();
        try {
            if (this.pendingNotifyStatus.isEmpty()) {
                return;
            }
            // Find all statuses that log index less than or equal to appliedIndex.
            final Map<Long, List<ReadIndexStatus>> statuses = this.pendingNotifyStatus.headMap(appliedIndex, true);
            if (statuses != null) {
                pendingStatuses = new ArrayList<>(statuses.size() << 1);

                final Iterator<Map.Entry<Long, List<ReadIndexStatus>>> it = statuses.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<Long, List<ReadIndexStatus>> entry = it.next();
                    pendingStatuses.addAll(entry.getValue());
                    // Remove the entry from statuses, it will also be removed in pendingNotifyStatus.
                    it.remove();
                }

            }

            /*
             * Remaining pending statuses are notified by error if it is presented.
             * When the node is in error state, consider following situations:
             * 1. If commitIndex > appliedIndex, then all pending statuses should be notified by error status.
             * 2. When commitIndex == appliedIndex, there will be no more pending statuses.
             */
            if (this.error != null) {
                resetPendingStatusError(this.error.getStatus());
            }
        }
        finally {
            this.lock.unlock();
            if (pendingStatuses != null && !pendingStatuses.isEmpty()) {
                for (final ReadIndexStatus status : pendingStatuses) {
                    notifySuccess(status);
                }
            }
        }
    }

    /**
     * Flush all events in disruptor.
     */
    @OnlyForTest
    void flush() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        this.readIndexQueue.publishEvent((task, sequence) -> task.shutdownLatch = latch);
        latch.await();
    }

    @OnlyForTest
    TreeMap<Long, List<ReadIndexStatus>> getPendingNotifyStatus() {
        return this.pendingNotifyStatus;
    }

    private void reportError(final ReadIndexStatus status, final Status st) {
        final long nowMs = Utils.monotonicMs();
        final List<ReadIndexState> states = status.getStates();
        final int taskCount = states.size();
        for (int i = 0; i < taskCount; i++) {
            final ReadIndexState task = states.get(i);
            final ReadIndexClosure done = task.getDone();
            if (done != null) {
                this.nodeMetrics.recordLatency("read-index", nowMs - task.getStartTimeMs());
                done.run(st);
            }
        }
    }

    private void notifySuccess(final ReadIndexStatus status) {
        final long nowMs = Utils.monotonicMs();
        final List<ReadIndexState> states = status.getStates();
        final int taskCount = states.size();
        for (int i = 0; i < taskCount; i++) {
            final ReadIndexState task = states.get(i);
            final ReadIndexClosure done = task.getDone(); // stack copy
            if (done != null) {
                this.nodeMetrics.recordLatency("read-index", nowMs - task.getStartTimeMs());
                done.setResult(task.getIndex(), task.getRequestContext().get());
                done.run(Status.OK());
            }
        }
    }
}
