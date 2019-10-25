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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lifecycle.LifecycleAware;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.DEFAULT_CAPACITY;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LockTrackerFactory.DEFAULT_TYPE;

//TODO Fast local get thread local.
//TODO Dynamic enable/disable tracing.
//TODO Collect page content to dump. AG
/**
 *
 */
public class SharedPageLockTracker implements LifecycleAware, PageLockListener, DumpSupported<ThreadPageLocksDumpLock> {
    /**
     *
     */
    private static final long OVERHEAD_SIZE = 16 + (8 * 8) + (4 * 3);

    /**
     *
     */
    private final MemoryCalculator memCalc;

    /**
     *
     */
    public final int threadLimits;

    /**
     *
     */
    public final int timeOutWorkerInterval;

    /**
     *
     */
    private final Map<Long, PageLockTracker<? extends PageLockDump>> threadStacks = new HashMap<>();

    /**
     *
     */
    private final Map<Long, Thread> threadIdToThreadRef = new HashMap<>();

    /**
     *
     */
    private final Map<String, Integer> structureNameToId = new HashMap<>();

    /** Thread for clean terminated threads from map. */
    private final TimeOutWorker timeOutWorker = new TimeOutWorker();

    /**
     *
     */
    private Map<Long, SharedPageLockTracker.State> prevThreadsState = new HashMap<>();

    /**
     *
     */
    private int idGen;

    /**
     *
     */
    private final Consumer<Set<SharedPageLockTracker.State>> hangThreadsCallBack;

    /**
     *
     */
    private final ThreadLocal<PageLockTracker> lockTracker = ThreadLocal.withInitial(this::createTracker);

    /**
     *
     */
    public SharedPageLockTracker() {
        this((ids) -> {
        }, new MemoryCalculator());
    }

    /**
     *
     */
    public SharedPageLockTracker(Consumer<Set<State>> hangThreadsCallBack, MemoryCalculator memCalc) {
        this(
            1000,
            getInteger(IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL, 60_000),
            hangThreadsCallBack,
            memCalc
        );
    }

    /**
     *
     */
    public SharedPageLockTracker(
        int threadLimits,
        int timeOutWorkerInterval,
        Consumer<Set<SharedPageLockTracker.State>> hangThreadsCallBack,
        MemoryCalculator memCalc
    ) {
        this.threadLimits = threadLimits;
        this.timeOutWorkerInterval = timeOutWorkerInterval;
        this.hangThreadsCallBack = hangThreadsCallBack;
        this.memCalc = memCalc;

        this.memCalc.onHeapAllocated(OVERHEAD_SIZE);
    }

    /**
     * Factory method for creating thread local {@link PageLockTracker}.
     *
     * @return PageLockTracer instance.
     */
    private PageLockTracker createTracker() {
        Thread thread = Thread.currentThread();

        String name = "name=" + thread.getName();
        long threadId = thread.getId();

        PageLockTracker<? extends PageLockDump> tracker = LockTrackerFactory.create(
            DEFAULT_TYPE, DEFAULT_CAPACITY, name, memCalc
        );

        synchronized (this) {
            threadStacks.put(threadId, tracker);

            threadIdToThreadRef.put(threadId, thread);

            memCalc.onHeapAllocated(((8 + 16 + 8) + 8) * 2);

            if (threadIdToThreadRef.size() > threadLimits)
                cleanTerminatedThreads();
        }

        return tracker;
    }

    /**
     *
     */
    public synchronized PageLockListener registrateStructure(String structureName) {
        Integer id = structureNameToId.get(structureName);

        if (id == null) {
            structureNameToId.put(structureName, id = (++idGen));

            // Size for new (K,V) pair.
            memCalc.onHeapAllocated((structureName.getBytes().length + 16) + (8 + 16 + 4));
        }

        // Size for PageLockListenerIndexAdapter object.
        memCalc.onHeapAllocated(16 + 4 + 8);

        return new PageLockListenerIndexAdapter(id, this);
    }

    /** {@inheritDoc} */
    @Override public void onBeforeWriteLock(int structureId, long pageId, long page) {
        lockTracker.get().onBeforeWriteLock(structureId, pageId, page);
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onWriteLock(structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onWriteUnlock(structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onBeforeReadLock(int structureId, long pageId, long page) {
        lockTracker.get().onBeforeReadLock(structureId, pageId, page);
    }

    /** {@inheritDoc} */
    @Override public void onReadLock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onReadLock(structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onReadUnlock(structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public synchronized ThreadPageLocksDumpLock dump() {
        Collection<PageLockTracker<? extends PageLockDump>> trackers = threadStacks.values();
        List<ThreadPageLocksDumpLock.ThreadState> threadStates = new ArrayList<>(threadStacks.size());

        for (PageLockTracker tracker : trackers) {
            boolean acquired = tracker.acquireSafePoint();

            //TODO
            assert acquired;
        }

        for (Map.Entry<Long, PageLockTracker<? extends PageLockDump>> entry : threadStacks.entrySet()) {
            Long threadId = entry.getKey();
            Thread thread = threadIdToThreadRef.get(threadId);

            PageLockTracker<? extends PageLockDump> tracker = entry.getValue();

            try {
                PageLockDump pageLockDump = tracker.dump();

                threadStates.add(
                    new ThreadPageLocksDumpLock.ThreadState(
                        threadId,
                        thread.getName(),
                        thread.getState(),
                        pageLockDump,
                        tracker.isInvalid() ? tracker.invalidContext() : null
                    )
                );
            }
            finally {
                tracker.releaseSafePoint();
            }
        }

        Map<Integer, String> idToStructureName0 =
            Collections.unmodifiableMap(
                structureNameToId.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getValue,
                        Map.Entry::getKey
                    ))
            );

        List<ThreadPageLocksDumpLock.ThreadState> threadStates0 =
            Collections.unmodifiableList(threadStates);

        // Get first thread dump time or current time is threadStates is empty.
        long time = !threadStates.isEmpty() ? threadStates.get(0).pageLockDump.time() : System.currentTimeMillis();

        return new ThreadPageLocksDumpLock(time, idToStructureName0, threadStates0);
    }

    /**
     *
     */
    private synchronized void cleanTerminatedThreads() {
        Iterator<Map.Entry<Long, Thread>> it = threadIdToThreadRef.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Long, Thread> entry = it.next();

            long threadId = entry.getKey();
            Thread thread = entry.getValue();

            if (thread.getState() == Thread.State.TERMINATED) {
                PageLockTracker tracker = threadStacks.remove(threadId);

                if (tracker != null) {
                    memCalc.onHeapFree((8 + 16 + 8) + 8);

                    tracker.free();
                }

                it.remove();

                memCalc.onHeapFree((8 + 16 + 8) + 8);
            }
        }
    }

    /**
     *
     */
    private synchronized Map<Long, State> getThreadOperationState() {
        return threadStacks.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> {
                PageLockTracker<? extends PageLockDump> lt = e.getValue();

                return new State(lt.operationsCounter(), lt.heldLocksNumber(), threadIdToThreadRef.get(e.getKey()));
            }
        ));
    }

    /**
     *
     */
    private synchronized Set<State> hangThreads() {
        Set<State> hangsThreads = new HashSet<>();

        Map<Long, SharedPageLockTracker.State> currentThreadsOperationState = getThreadOperationState();

        prevThreadsState.forEach((threadId, prevState) -> {
            State state = currentThreadsOperationState.get(threadId);

            if (state == null)
                return;

            boolean threadHoldedLocks = state.heldLockCnt != 0;

            // If thread holds a lock and does not change state it may be hanged.
            if (prevState.equals(state) && threadHoldedLocks)
                hangsThreads.add(state);
        });

        prevThreadsState = currentThreadsOperationState;

        return hangsThreads;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        timeOutWorker.setDaemon(true);

        timeOutWorker.start();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        timeOutWorker.interrupt();

        try {
            timeOutWorker.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException(e);
        }
    }

    /**
     *
     */
    private class TimeOutWorker extends Thread {
        /**
         *
         */
        @Override public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    sleep(timeOutWorkerInterval);

                    cleanTerminatedThreads();

                    if (hangThreadsCallBack != null) {
                        Set<SharedPageLockTracker.State> threadIds = hangThreads();

                        if (!F.isEmpty(threadIds))
                            hangThreadsCallBack.accept(threadIds);
                    }
                }
            }
            catch (InterruptedException e) {
                // No-op.
            }
        }
    }

    /**
     *
     */
    public static class State {
        /**
         *
         */
        final long threadOpCnt;

        /**
         *
         */
        final long heldLockCnt;

        /**
         *
         */
        final Thread thread;

        /**
         *
         */
        private State(long threadOpCnt, long heldLockCnt, Thread thread) {
            this.threadOpCnt = threadOpCnt;
            this.heldLockCnt = heldLockCnt;
            this.thread = thread;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            State state = (State)o;
            return threadOpCnt == state.threadOpCnt &&
                heldLockCnt == state.heldLockCnt &&
                Objects.equals(thread, state.thread);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(threadOpCnt, heldLockCnt, thread);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<ThreadPageLocksDumpLock> dumpSync() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean acquireSafePoint() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean releaseSafePoint() {
        throw new UnsupportedOperationException();
    }
}
