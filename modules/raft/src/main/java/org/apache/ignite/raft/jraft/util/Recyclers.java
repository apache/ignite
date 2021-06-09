/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Light-weight object pool based on a thread-local stack.
 * <p/>
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @param <T> the type of the pooled object
 */
public abstract class Recyclers<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Recyclers.class);

    private static final AtomicInteger idGenerator = new AtomicInteger(Integer.MIN_VALUE);

    private static final int OWN_THREAD_ID = idGenerator.getAndIncrement();
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024; // Use 4k instances as default.
    private static final int MAX_CAPACITY_PER_THREAD;
    private static final int INITIAL_CAPACITY;

    static {
        int maxCapacityPerThread = SystemPropertyUtil.getInt("jraft.recyclers.maxCapacityPerThread", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD);
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;

        LOG.info("-Djraft.recyclers.maxCapacityPerThread: {}.", MAX_CAPACITY_PER_THREAD == 0 ? "disabled" : MAX_CAPACITY_PER_THREAD);

        INITIAL_CAPACITY = Math.min(MAX_CAPACITY_PER_THREAD, 256);
    }

    public static final Handle NOOP_HANDLE = new Handle() {
    };

    private final int maxCapacityPerThread;
    private final ThreadLocal<Stack<T>> threadLocal = new ThreadLocal<Stack<T>>() {

        @Override
        protected Stack<T> initialValue() {
            return new Stack<>(Recyclers.this, Thread.currentThread(), maxCapacityPerThread);
        }
    };

    protected Recyclers() {
        this(MAX_CAPACITY_PER_THREAD);
    }

    protected Recyclers(int maxCapacityPerThread) {
        this.maxCapacityPerThread = Math.min(MAX_CAPACITY_PER_THREAD, Math.max(0, maxCapacityPerThread));
    }

    public final T get() {
        if (maxCapacityPerThread == 0) {
            return newObject(NOOP_HANDLE);
        }
        Stack<T> stack = threadLocal.get();
        DefaultHandle handle = stack.pop();
        if (handle == null) {
            handle = stack.newHandle();
            handle.value = newObject(handle);
        }
        return (T) handle.value;
    }

    public final boolean recycle(T o, Handle handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        DefaultHandle h = (DefaultHandle) handle;

        final Stack<?> stack = h.stack;
        if (h.lastRecycledId != h.recycleId || stack == null) {
            throw new IllegalStateException("recycled already");
        }

        if (stack.parent != this) {
            return false;
        }
        if (o != h.value) {
            throw new IllegalArgumentException("o does not belong to handle");
        }
        h.recycle();
        return true;
    }

    protected abstract T newObject(Handle handle);

    public final int threadLocalCapacity() {
        return threadLocal.get().elements.length;
    }

    public final int threadLocalSize() {
        return threadLocal.get().size;
    }

    public interface Handle {
    }

    static final class DefaultHandle implements Handle {
        private int lastRecycledId;
        private int recycleId;

        private Stack<?> stack;
        private Object value;

        DefaultHandle(Stack<?> stack) {
            this.stack = stack;
        }

        public void recycle() {
            Thread thread = Thread.currentThread();

            final Stack<?> stack = this.stack;
            if (lastRecycledId != recycleId || stack == null) {
                throw new IllegalStateException("recycled already");
            }

            if (thread == stack.thread) {
                stack.push(this);
                return;
            }
            // we don't want to have a ref to the queue as the value in our weak map
            // so we null it out; to ensure there are no races with restoring it later
            // we impose a memory ordering here (no-op on x86)
            Map<Stack<?>, WeakOrderQueue> delayedRecycled = Recyclers.delayedRecycled.get();
            WeakOrderQueue queue = delayedRecycled.get(stack);
            if (queue == null) {
                delayedRecycled.put(stack, queue = new WeakOrderQueue(stack, thread));
            }
            queue.add(this);
        }
    }

    private static final ThreadLocal<Map<Stack<?>, WeakOrderQueue>> delayedRecycled = ThreadLocal.withInitial(WeakHashMap::new);

    // a queue that makes only moderate guarantees about visibility: items are seen in the correct order,
    // but we aren't absolutely guaranteed to ever see anything at all, thereby keeping the queue cheap to maintain
    private static final class WeakOrderQueue {
        private static final int LINK_CAPACITY = 16;

        // Let Link extend AtomicInteger for intrinsics. The Link itself will be used as writerIndex.
        @SuppressWarnings("serial")
        private static final class Link extends AtomicInteger {
            private final DefaultHandle[] elements = new DefaultHandle[LINK_CAPACITY];

            private int readIndex;
            private Link next;
        }

        // chain of data items
        private Link head, tail;
        // pointer to another queue of delayed items for the same stack
        private WeakOrderQueue next;
        private final WeakReference<Thread> owner;
        private final int id = idGenerator.getAndIncrement();

        WeakOrderQueue(Stack<?> stack, Thread thread) {
            head = tail = new Link();
            owner = new WeakReference<>(thread);
            synchronized (stackLock(stack)) {
                next = stack.head;
                stack.head = this;
            }
        }

        private Object stackLock(Stack<?> stack) {
            return stack;
        }

        void add(DefaultHandle handle) {
            handle.lastRecycledId = id;

            Link tail = this.tail;
            int writeIndex;
            if ((writeIndex = tail.get()) == LINK_CAPACITY) {
                this.tail = tail = tail.next = new Link();
                writeIndex = tail.get();
            }
            tail.elements[writeIndex] = handle;
            handle.stack = null;
            // we lazy set to ensure that setting stack to null appears before we unnull it in the owning thread;
            // this also means we guarantee visibility of an element in the queue if we see the index updated
            tail.lazySet(writeIndex + 1);
        }

        boolean hasFinalData() {
            return tail.readIndex != tail.get();
        }

        // transfer as many items as we can from this queue to the stack, returning true if any were transferred
        boolean transfer(Stack<?> dst) {
            Link head = this.head;
            if (head == null) {
                return false;
            }

            if (head.readIndex == LINK_CAPACITY) {
                if (head.next == null) {
                    return false;
                }
                this.head = head = head.next;
            }

            final int srcStart = head.readIndex;
            int srcEnd = head.get();
            final int srcSize = srcEnd - srcStart;
            if (srcSize == 0) {
                return false;
            }

            final int dstSize = dst.size;
            final int expectedCapacity = dstSize + srcSize;

            if (expectedCapacity > dst.elements.length) {
                final int actualCapacity = dst.increaseCapacity(expectedCapacity);
                srcEnd = Math.min(srcStart + actualCapacity - dstSize, srcEnd);
            }

            if (srcStart != srcEnd) {
                final DefaultHandle[] srcElems = head.elements;
                final DefaultHandle[] dstElems = dst.elements;
                int newDstSize = dstSize;
                for (int i = srcStart; i < srcEnd; i++) {
                    DefaultHandle element = srcElems[i];
                    if (element.recycleId == 0) {
                        element.recycleId = element.lastRecycledId;
                    }
                    else if (element.recycleId != element.lastRecycledId) {
                        throw new IllegalStateException("recycled already");
                    }
                    element.stack = dst;
                    dstElems[newDstSize++] = element;
                    srcElems[i] = null;
                }
                dst.size = newDstSize;

                if (srcEnd == LINK_CAPACITY && head.next != null) {
                    this.head = head.next;
                }

                head.readIndex = srcEnd;
                return true;
            }
            else {
                // The destination stack is full already.
                return false;
            }
        }
    }

    static final class Stack<T> {

        // we keep a queue of per-thread queues, which is appended to once only, each time a new thread other
        // than the stack owner recycles: when we run out of items in our stack we iterate this collection
        // to scavenge those that can be reused. this permits us to incur minimal thread synchronisation whilst
        // still recycling all items.
        final Recyclers<T> parent;
        final Thread thread;
        private DefaultHandle[] elements;
        private final int maxCapacity;
        private int size;

        private volatile WeakOrderQueue head;
        private WeakOrderQueue cursor, prev;

        Stack(Recyclers<T> parent, Thread thread, int maxCapacity) {
            this.parent = parent;
            this.thread = thread;
            this.maxCapacity = maxCapacity;
            elements = new DefaultHandle[Math.min(INITIAL_CAPACITY, maxCapacity)];
        }

        int increaseCapacity(int expectedCapacity) {
            int newCapacity = elements.length;
            int maxCapacity = this.maxCapacity;
            do {
                newCapacity <<= 1;
            }
            while (newCapacity < expectedCapacity && newCapacity < maxCapacity);

            newCapacity = Math.min(newCapacity, maxCapacity);
            if (newCapacity != elements.length) {
                elements = Arrays.copyOf(elements, newCapacity);
            }

            return newCapacity;
        }

        DefaultHandle pop() {
            int size = this.size;
            if (size == 0) {
                if (!scavenge()) {
                    return null;
                }
                size = this.size;
            }
            size--;
            DefaultHandle ret = elements[size];
            if (ret.lastRecycledId != ret.recycleId) {
                throw new IllegalStateException("recycled multiple times");
            }
            ret.recycleId = 0;
            ret.lastRecycledId = 0;
            this.size = size;
            return ret;
        }

        boolean scavenge() {
            // continue an existing scavenge, if any
            if (scavengeSome()) {
                return true;
            }

            // reset our scavenge cursor
            prev = null;
            cursor = head;
            return false;
        }

        boolean scavengeSome() {
            WeakOrderQueue cursor = this.cursor;
            if (cursor == null) {
                cursor = head;
                if (cursor == null) {
                    return false;
                }
            }

            boolean success = false;
            WeakOrderQueue prev = this.prev;
            do {
                if (cursor.transfer(this)) {
                    success = true;
                    break;
                }

                WeakOrderQueue next = cursor.next;
                if (cursor.owner.get() == null) {
                    // If the thread associated with the queue is gone, unlink it, after
                    // performing a volatile read to confirm there is no data left to collect.
                    // We never unlink the first queue, as we don't want to synchronize on updating the head.
                    if (cursor.hasFinalData()) {
                        for (; ; ) {
                            if (cursor.transfer(this)) {
                                success = true;
                            }
                            else {
                                break;
                            }
                        }
                    }
                    if (prev != null) {
                        prev.next = next;
                    }
                }
                else {
                    prev = cursor;
                }

                cursor = next;

            }
            while (cursor != null && !success);

            this.prev = prev;
            this.cursor = cursor;
            return success;
        }

        void push(DefaultHandle item) {
            if ((item.recycleId | item.lastRecycledId) != 0) {
                throw new IllegalStateException("recycled already");
            }
            item.recycleId = item.lastRecycledId = OWN_THREAD_ID;

            int size = this.size;
            if (size >= maxCapacity) {
                // Hit the maximum capacity - drop the possibly youngest object.
                return;
            }
            if (size == elements.length) {
                elements = Arrays.copyOf(elements, Math.min(size << 1, maxCapacity));
            }

            elements[size] = item;
            this.size = size + 1;
        }

        DefaultHandle newHandle() {
            return new DefaultHandle(this);
        }
    }
}
