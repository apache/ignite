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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;

//TODO Calculate overhad and capacity for all structures.
//TODO Fast local get thread local.
//TODO Test deadlock
//TODO Dynamic enable/disable tracing.
//TODO Collect page content to dump. AG
//TODO Create dump by timeout.

/** */
public class SharedPageLockTracker implements PageLockListener, DumpSupported<ThreadPageLocksDumpLock> {
    /** */
    private static final int THREAD_LIMITS = 1000;

    /** */
    private static final int TIME_OUT_WORKER_INTERVAL = 60_000;

    /** */
    private final Map<Long, PageLockTracker<? extends PageLockDump>> threadStacks = new HashMap<>();
    /** */
    private final Map<Long, Thread> threadIdToThreadRef = new HashMap<>();

    /** */
    private final Map<String, Integer> structureNameToId = new HashMap<>();

    /** Thread for clean terminated threads from map. */
    private final TimeOutWorker timeOutWorker = new TimeOutWorker();

    /** */
    private int idGen;

    /** */
    private final ThreadLocal<PageLockTracker> lockTracker = ThreadLocal.withInitial(() -> {
        Thread thread = Thread.currentThread();

        String threadName = thread.getName();
        long threadId = thread.getId();

        PageLockTracker<? extends PageLockDump> tracker = LockTrackerFactory.create("name=" + threadName);

        synchronized (this) {
            threadStacks.put(threadId, tracker);

            threadIdToThreadRef.put(threadId, thread);

            if (threadIdToThreadRef.size() > THREAD_LIMITS)
                cleanTerminatedThreads();
        }

        return tracker;
    });

    /** */
    void onStart() {
        timeOutWorker.setDaemon(true);

        timeOutWorker.start();
    }

    /** */
    public synchronized PageLockListener registrateStructure(String structureName) {
        Integer id = structureNameToId.get(structureName);

        if (id == null)
            structureNameToId.put(structureName, id = (++idGen));

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

    /** */
    private synchronized void cleanTerminatedThreads() {
        Iterator<Map.Entry<Long, Thread>> it = threadIdToThreadRef.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<Long, Thread> entry = it.next();

            long threadId = entry.getKey();
            Thread thread = entry.getValue();

            if (thread.getState() == Thread.State.TERMINATED) {
                PageLockTracker tracker = threadStacks.remove(threadId);

                if (tracker != null)
                    tracker.free();

                it.remove();
            }
        }
    }

    /** */
    private synchronized Map<Long, Long> getThreadOperationCounters() {
        return threadStacks.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().operationsCounter()
        ));
    }

    /** */
    private class TimeOutWorker extends Thread {
        /** */
        private Map<Long, Long> prevThreadsOperationCounters = new HashMap<>();

        @Override public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    sleep(TIME_OUT_WORKER_INTERVAL);

                    cleanTerminatedThreads();

                    List<Long> hangsThreads = null;

                    Map<Long, Long> currentThreadsOperationCounters = getThreadOperationCounters();

                    for (Map.Entry<Long, Long> entry : prevThreadsOperationCounters.entrySet()) {
                        Long threadId = entry.getKey();

                        Long currOpCnt = currentThreadsOperationCounters.get(threadId);

                        if (currOpCnt == null)
                            continue;

                        Long prevOpCnt = entry.getValue();

                        if (prevOpCnt.equals(currOpCnt)) {
                            if (hangsThreads == null)
                                hangsThreads = new ArrayList<>();

                            hangsThreads.add(threadId);
                        }
                    }

                    if (!F.isEmpty(hangsThreads)) {
                        // TODO.
                    }

                    prevThreadsOperationCounters = currentThreadsOperationCounters;
                }
            }
            catch (InterruptedException e) {
                // No-op.
            }
        }
    }

    /** */
    @Override public IgniteFuture<ThreadPageLocksDumpLock> dumpSync() {
        throw new UnsupportedOperationException();
    }

    /** */
    @Override public boolean acquireSafePoint() {
        throw new UnsupportedOperationException();
    }

    /** */
    @Override public boolean releaseSafePoint() {
        throw new UnsupportedOperationException();
    }
}
