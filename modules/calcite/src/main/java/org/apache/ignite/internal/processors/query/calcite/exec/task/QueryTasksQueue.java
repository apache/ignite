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

package org.apache.ignite.internal.processors.query.calcite.exec.task;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A tasks queue with filtering (based on linked nodes).
 */
class QueryTasksQueue {
    /**
     * Linked list node class.
     */
    private static class Node {
        /** */
        QueryAwareTask item;

        /** Next node in chain. */
        Node next;

        /** */
        Node(QueryAwareTask item) {
            this.item = item;
        }
    }

    /** Current number of elements */
    private final AtomicInteger cnt = new AtomicInteger();

    /** Head of linked list. */
    Node head;

    /** Tail of linked list. */
    private Node last;

    /** */
    private final ReentrantLock lock = new ReentrantLock();

    /** Wait condition for waiting takes. */
    private final Condition notEmpty = lock.newCondition();

    /** Set of blocked (currently running) queries. */
    private final Set<QueryKey> blockedQrys = new HashSet<>();

    /**
     * Creates a {@code LinkedBlockingQueue}.
     */
    QueryTasksQueue() {
        last = head = new Node(null);
    }

    /** Queue size. */
    public int size() {
        return cnt.get();
    }

    /** Add a task to the queue. */
    public void addTask(QueryAwareTask task) {
        lock.lock();

        try {
            assert last.next == null : "Unexpected last.next: " + last.next;

            last = last.next = new Node(task);

            cnt.getAndIncrement();

            notEmpty.signal();
        }
        finally {
            lock.unlock();
        }
    }

    /** Poll task and block query. */
    public QueryAwareTask pollTaskAndBlockQuery(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lockInterruptibly();

        try {
            QueryAwareTask res;

            long nanos = unit.toNanos(timeout);
            final AtomicInteger cnt = this.cnt;

            while (cnt.get() == 0 || (res = dequeue()) == null) {
                if (nanos <= 0L)
                    return null;

                nanos = notEmpty.awaitNanos(nanos);
            }

            blockedQrys.add(res.queryKey());

            if (cnt.decrementAndGet() > 0)
                notEmpty.signal();

            return res;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes a first non-blocked task from the head of the queue.
     *
     * @return The task.
     */
    private QueryAwareTask dequeue() {
        assert lock.isHeldByCurrentThread();
        assert head.item == null : "Unexpected head.item: " + head.item;

        for (Node pred = head, cur = pred.next; cur != null; pred = cur, cur = cur.next) {
            if (!blockedQrys.contains(cur.item.queryKey())) { // Skip tasks for blocked queries.
                QueryAwareTask res = cur.item;

                unlink(pred, cur);

                return res;
            }
        }

        return null;
    }

    /** Unblock query. */
    public void unblockQuery(QueryKey qryKey) {
        lock.lock();

        try {
            blockedQrys.remove(qryKey);

            if (cnt.get() > 0)
                notEmpty.signal();
        }
        finally {
            lock.unlock();
        }
    }

    /** Remove task */
    public boolean removeTask(QueryAwareTask task) {
        if (task == null)
            return false;

        lock.lock();

        try {
            for (Node pred = head, cur = pred.next; cur != null; pred = cur, cur = cur.next) {
                if (task.equals(cur.item)) {
                    unlink(pred, cur);

                    cnt.getAndDecrement();

                    return true;
                }
            }

            return false;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Unlinks interior Node cur with predecessor pred.
     */
    private void unlink(Node pred, Node cur) {
        cur.item = null;
        pred.next = cur.next;

        if (last == cur)
            last = pred;
    }

    /** */
    public <T> T[] toArray(T[] a) {
        lock.lock();

        try {
            int size = cnt.get();

            if (a.length < size)
                a = (T[])Array.newInstance(a.getClass().getComponentType(), size);

            int k = 0;

            for (Node cur = head.next; cur != null; cur = cur.next)
                a[k++] = (T)cur.item;

            while (a.length > k)
                a[k++] = null;

            return a;
        }
        finally {
            lock.unlock();
        }
    }

    /** */
    public int drainTo(Collection<? super QueryAwareTask> c, int maxElements) {
        Objects.requireNonNull(c);

        if (maxElements <= 0)
            return 0;

        lock.lock();

        try {
            int n = Math.min(maxElements, cnt.get());
            int i = 0;

            for (Node cur = head.next; i < n && cur != null; cur = cur.next, i++) {
                c.add(cur.item);

                unlink(head, cur);

                cnt.getAndDecrement();
            }

            return i;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @return {@code BlockingQueue} on top of {@code QueryTasksQueue}. This blocking queue implements only methods
     * required by {@code ThreadPoolExecutor}.
     */
    public BlockingQueue<Runnable> blockingQueue() {
        return new BlockingQueue<>() {
            @Override public boolean add(@NotNull Runnable runnable) {
                addTask((QueryAwareTask)runnable);

                return true;
            }

            @Override public boolean offer(@NotNull Runnable runnable) {
                return add(runnable);
            }

            @Override public boolean offer(Runnable runnable, long timeout, @NotNull TimeUnit unit) {
                return add(runnable);
            }

            @Override public void put(@NotNull Runnable runnable) {
                add(runnable);
            }

            @Override public int remainingCapacity() {
                return Integer.MAX_VALUE;
            }

            @Override public boolean remove(Object o) {
                return removeTask((QueryAwareTask)o);
            }

            @Override public @NotNull Runnable take() throws InterruptedException {
                return pollTaskAndBlockQuery(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }

            @Override public @Nullable Runnable poll(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
                return pollTaskAndBlockQuery(0, TimeUnit.NANOSECONDS);
            }

            @Override public Runnable remove() {
                throw new UnsupportedOperationException();
            }

            @Override public Runnable poll() {
                throw new UnsupportedOperationException();
            }

            @Override public int size() {
                return QueryTasksQueue.this.size();
            }

            @Override public boolean isEmpty() {
                return size() == 0;
            }

            @Override public @NotNull Object[] toArray() {
                return toArray(new Object[size()]);
            }

            @Override public @NotNull <T> T[] toArray(@NotNull T[] a) {
                return QueryTasksQueue.this.toArray(a);
            }

            @Override public int drainTo(@NotNull Collection<? super Runnable> c) {
                return drainTo(c, Integer.MAX_VALUE);
            }

            @Override public int drainTo(@NotNull Collection<? super Runnable> c, int maxElements) {
                return QueryTasksQueue.this.drainTo(c, maxElements);
            }

            @Override public boolean contains(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override public Runnable element() {
                throw new UnsupportedOperationException();
            }

            @Override public Runnable peek() {
                throw new UnsupportedOperationException();
            }

            @Override public @NotNull Iterator<Runnable> iterator() {
                throw new UnsupportedOperationException();
            }

            @Override public boolean containsAll(@NotNull Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override public boolean addAll(@NotNull Collection<? extends Runnable> c) {
                throw new UnsupportedOperationException();
            }

            @Override public boolean removeAll(@NotNull Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override public boolean retainAll(@NotNull Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override public void clear() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
