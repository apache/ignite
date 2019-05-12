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
import org.apache.ignite.lang.IgniteFuture;

/**
 * //TODO Calculate overhad and capacity for all structures.
 * //TODO Fast local get thread local.
 * //TODO Test deadlock
 * //TODO Dynamic enable/disable tracing.
 * //TODO Collect page content to dump. AG
 */
public class SharedPageLockTracker implements PageLockListener, DumpSupported<ThreadDumpLocks> {
    private static final int THREAD_LIMITS = 1000;

    private final Map<Long, PageLockTracker> threadStacks = new HashMap<>();
    private final Map<Long, Thread> threadIdToThreadRef = new HashMap<>();

    private final Map<String, Integer> structureNameToId = new HashMap<>();

    private final Cleaner cleaner = new Cleaner();

    private int idGen;

    /** */
    private final ThreadLocal<PageLockTracker> lockTracker = ThreadLocal.withInitial(() -> {
        Thread thread = Thread.currentThread();

        String threadName = thread.getName();
        long threadId = thread.getId();

        PageLockTracker tracker = LockTracerFactory.create(threadName + "[" + threadId + "]");

        synchronized (this) {
            threadStacks.put(threadId, tracker);

            threadIdToThreadRef.put(threadId, thread);

            if (threadIdToThreadRef.size() > THREAD_LIMITS)
                cleanTerminatedThreads();
        }

        return tracker;
    });

    void onStart() {
        cleaner.setDaemon(true);

        cleaner.start();
    }

    public synchronized PageLockListener registrateStructure(String structureName) {
        Integer id = structureNameToId.get(structureName);

        if (id == null)
            structureNameToId.put(structureName, id = (++idGen));

        return new PageLockListenerIndexAdapter(id, this);
    }

    @Override public void onBeforeWriteLock(int structureId, long pageId, long page) {
        lockTracker.get().onBeforeWriteLock(structureId, pageId, page);
    }

    @Override public void onWriteLock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onWriteLock(structureId, pageId, page, pageAddr);
    }

    @Override public void onWriteUnlock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onWriteUnlock(structureId, pageId, page, pageAddr);
    }

    @Override public void onBeforeReadLock(int structureId, long pageId, long page) {
        lockTracker.get().onBeforeReadLock(structureId, pageId, page);
    }

    @Override public void onReadLock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onReadLock(structureId, pageId, page, pageAddr);
    }

    @Override public void onReadUnlock(int structureId, long pageId, long page, long pageAddr) {
        lockTracker.get().onReadUnlock(structureId, pageId, page, pageAddr);
    }

    @Override public synchronized ThreadDumpLocks dump() {
        Collection<PageLockTracker> trackers = threadStacks.values();
        List<ThreadDumpLocks.ThreadState> threadStates = new ArrayList<>(threadStacks.size());

        for (PageLockTracker tracker : trackers) {
            boolean acquired = tracker.acquireSafePoint();

            //TODO
            assert acquired;
        }

        for (Map.Entry<Long, PageLockTracker> entry : threadStacks.entrySet()) {
            Long threadId = entry.getKey();
            Thread thread = threadIdToThreadRef.get(threadId);

            PageLockTracker<Dump> tracker = entry.getValue();

            try {
                Dump dump = tracker.dump();

                threadStates.add(
                    new ThreadDumpLocks.ThreadState(
                        threadId,
                        thread.getName(),
                        thread.getState(),
                        dump,
                        tracker.isInvalid() ? tracker.invalidContext() : null
                    )
                );
            }
            finally {
                tracker.releaseSafePoint();
            }
        }

        Map<Integer, String> idToStrcutureName0 =
            Collections.unmodifiableMap(
                structureNameToId.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getValue,
                        Map.Entry::getKey
                    ))
            );

        List<ThreadDumpLocks.ThreadState> threadStates0 =
            Collections.unmodifiableList(threadStates);

        return new ThreadDumpLocks(idToStrcutureName0, threadStates0);
    }

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

    private class Cleaner extends Thread {
        @Override public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    sleep(60_000);

                    cleanTerminatedThreads();
                }
            }
            catch (InterruptedException e) {
                // No-op.
            }
        }
    }

    @Override public IgniteFuture<ThreadDumpLocks> dumpSync() {
        throw new UnsupportedOperationException();
    }

    @Override public boolean acquireSafePoint() {
        throw new UnsupportedOperationException();
    }

    @Override public boolean releaseSafePoint() {
        throw new UnsupportedOperationException();
    }
}
