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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.worker.CycleThread;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerFactory.DEFAULT_CAPACITY;
import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerFactory.DEFAULT_TYPE;

//TODO Fast local get thread local.
//TODO Dynamic enable/disable tracing.
//TODO Collect page content to dump. AG
/**
 *
 */
public class SharedPageLockTracker {
    /** */
    private static final long OVERHEAD_SIZE = 16 + (8 * 8) + (4 * 3);

    /** @see IgniteSystemProperties#IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL */
    public static final int DFLT_PAGE_LOCK_TRACKER_CHECK_INTERVAL = 60_000;

    /** */
    private final MemoryCalculator memCalc;

    /** */
    public final int threadLimits;

    /** */
    private final Map<Long, PageLockTracker<?>> threadStacks = new HashMap<>();

    /** */
    private final Map<Long, Thread> threadIdToThreadRef = new HashMap<>();

    /** */
    private final Map<String, Integer> structureNameToId = new ConcurrentHashMap<>();

    /** Thread for clean terminated threads from map. */
    private final TimeOutWorker timeOutWorker;

    /** */
    private Map<Long, PageLockThreadState> prevThreadsState = new HashMap<>();

    /** */
    private final AtomicInteger idGen = new AtomicInteger();

    /** */
    private final Consumer<Set<PageLockThreadState>> hangThreadsCallBack;

    /** */
    private final ThreadLocal<PageLockTracker<?>> lockTracker = ThreadLocal.withInitial(this::createTracker);

    /** */
    public SharedPageLockTracker() {
        this(ids -> {}, new MemoryCalculator());
    }

    /** */
    public SharedPageLockTracker(Consumer<Set<PageLockThreadState>> hangThreadsCallBack, MemoryCalculator memCalc) {
        this(
            1000,
            getInteger(IGNITE_PAGE_LOCK_TRACKER_CHECK_INTERVAL, DFLT_PAGE_LOCK_TRACKER_CHECK_INTERVAL),
            hangThreadsCallBack,
            memCalc
        );
    }

    /** */
    public SharedPageLockTracker(
        int threadLimits,
        int timeOutWorkerInterval,
        Consumer<Set<PageLockThreadState>> hangThreadsCallBack,
        MemoryCalculator memCalc
    ) {
        this.threadLimits = threadLimits;
        this.timeOutWorker = new TimeOutWorker(timeOutWorkerInterval);
        this.hangThreadsCallBack = hangThreadsCallBack;
        this.memCalc = memCalc;

        this.memCalc.onHeapAllocated(OVERHEAD_SIZE);
    }

    /**
     * Factory method for creating thread local {@link PageLockTracker}.
     *
     * @return PageLockTracer instance.
     */
    private PageLockTracker<?> createTracker() {
        Thread thread = Thread.currentThread();

        String name = "name=" + thread.getName();
        long threadId = thread.getId();

        PageLockTracker<?> tracker = PageLockTrackerFactory.create(
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

    /** */
    public PageLockListener registerStructure(String structureName) {
        int structureId = structureNameToId.computeIfAbsent(structureName, name -> {
            // Size for the new (K,V) pair.
            memCalc.onHeapAllocated((name.getBytes().length + 16) + (8 + 16 + 4));

            return idGen.incrementAndGet();
        });

        // Size for the PageLockListener object.
        memCalc.onHeapAllocated(16 + 4 + 8);

        return new PageLockListener() {
            @Override public void onBeforeWriteLock(int cacheId, long pageId, long page) {
                lockTracker.get().onBeforeWriteLock(structureId, pageId, page);
            }

            @Override public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
                lockTracker.get().onWriteLock(structureId, pageId, page, pageAddr);
            }

            @Override public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
                lockTracker.get().onWriteUnlock(structureId, pageId, page, pageAddr);
            }

            @Override public void onBeforeReadLock(int cacheId, long pageId, long page) {
                lockTracker.get().onBeforeReadLock(structureId, pageId, page);
            }

            @Override public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
                lockTracker.get().onReadLock(structureId, pageId, page, pageAddr);
            }

            @Override public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
                lockTracker.get().onReadUnlock(structureId, pageId, page, pageAddr);
            }

            @Override public void close() {
                structureNameToId.remove(structureName);
            }
        };
    }

    /**
     * Creates dump.
     */
    public synchronized SharedPageLockTrackerDump dump() {
        Collection<PageLockTracker<?>> trackers = threadStacks.values();
        List<ThreadPageLockState> threadPageLockStates = new ArrayList<>(threadStacks.size());

        for (PageLockTracker<?> tracker : trackers) {
            boolean acquired = tracker.acquireSafePoint();

            //TODO
            assert acquired;
        }

        for (Map.Entry<Long, PageLockTracker<?>> entry : threadStacks.entrySet()) {
            Long threadId = entry.getKey();
            Thread thread = threadIdToThreadRef.get(threadId);

            PageLockTracker<? extends PageLockDump> tracker = entry.getValue();

            try {
                PageLockDump pageLockDump = tracker.dump();

                threadPageLockStates.add(
                    new ThreadPageLockState(
                        threadId,
                        thread.getName(),
                        thread.getState(),
                        pageLockDump,
                        tracker.invalidContext()
                    )
                );
            }
            finally {
                tracker.releaseSafePoint();
            }
        }

        Map<Integer, String> idToStructureName = structureNameToId.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        // Get first thread dump time or current time is threadStates is empty.
        long time = !threadPageLockStates.isEmpty() ? threadPageLockStates.get(0).pageLockDump.time : System.currentTimeMillis();

        return new SharedPageLockTrackerDump(time, idToStructureName, threadPageLockStates);
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
                PageLockTracker<?> tracker = threadStacks.remove(threadId);

                if (tracker != null) {
                    memCalc.onHeapFree((8 + 16 + 8) + 8);

                    tracker.close();
                }

                it.remove();

                memCalc.onHeapFree((8 + 16 + 8) + 8);
            }
        }
    }

    /** */
    private Map<Long, PageLockThreadState> getThreadOperationState() {
        return threadStacks.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> {
                PageLockTracker<? extends PageLockDump> lt = e.getValue();

                return new PageLockThreadState(lt.operationsCounter(), lt.heldLocksNumber(), threadIdToThreadRef.get(e.getKey()));
            }
        ));
    }

    /** */
    private synchronized Set<PageLockThreadState> hangThreads() {
        Set<PageLockThreadState> hangsThreads = new HashSet<>();

        Map<Long, PageLockThreadState> curThreadsOperationState = getThreadOperationState();

        prevThreadsState.forEach((threadId, prevState) -> {
            PageLockThreadState state = curThreadsOperationState.get(threadId);

            if (state == null)
                return;

            boolean threadHoldedLocks = state.heldLockCnt != 0;

            // If thread holds a lock and does not change state it may be hanged.
            if (prevState.equals(state) && threadHoldedLocks)
                hangsThreads.add(state);
        });

        prevThreadsState = curThreadsOperationState;

        return hangsThreads;
    }

    /** Starts background worker. */
    public void start() {
        timeOutWorker.setDaemon(true);

        timeOutWorker.start();
    }

    /** Stops background worker. */
    public void stop() {
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
    private class TimeOutWorker extends CycleThread {

        /**
         *
         */
        TimeOutWorker(long interval) {
            super("page-lock-tracker-timeout", interval);
        }

        /** {@inheritDoc} */
        @Override public void iteration() {
            cleanTerminatedThreads();

            if (hangThreadsCallBack != null) {
                Set<PageLockThreadState> threadIds = hangThreads();

                if (!F.isEmpty(threadIds))
                    hangThreadsCallBack.accept(threadIds);
            }
        }
    }
}
