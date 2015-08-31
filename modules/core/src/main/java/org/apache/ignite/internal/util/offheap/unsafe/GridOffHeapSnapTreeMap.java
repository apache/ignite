/*
 * Copyright (c) 2009 Stanford University, unless otherwise specified.
 * All rights reserved.
 *
 * This software was developed by the Pervasive Parallelism Laboratory of
 * Stanford University, California, USA.
 *
 * Permission to use, copy, modify, and distribute this software in source
 * or binary form for any purpose with or without fee is hereby granted,
 * provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *    3. Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

package org.apache.ignite.internal.util.offheap.unsafe;

import java.io.Closeable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 *  A concurrent AVL tree with fast cloning, based on the algorithm of Bronson,
 *  Casper, Chafi, and Olukotun, "A Practical Concurrent Binary Search Tree"
 *  published in PPoPP'10.  To simplify the locking protocols rebalancing work
 *  is performed in pieces, and some removed keys are be retained as routing
 *  nodes in the tree.
 *
 *  <p>This data structure honors all of the contracts of {@link
 *  java.util.concurrent.ConcurrentSkipListMap}, with the additional contract
 *  that clone, size, toArray, and iteration are linearizable (atomic).
 *
 *  <p>The tree uses optimistic concurrency control.  No locks are usually
 *  required for get, containsKey, firstKey, firstEntry, lastKey, or lastEntry.
 *  Reads are not lock free (or even obstruction free), but obstructing threads
 *  perform no memory allocation, system calls, or loops, which seems to work
 *  okay in practice.  All of the updates to the tree are performed in fixed-
 *  size blocks, so restoration of the AVL balance criteria may occur after a
 *  change to the tree has linearized (but before the mutating operation has
 *  returned).  The tree is always properly balanced when quiescent.
 *
 *  <p>To clone the tree (or produce a snapshot for consistent iteration) the
 *  root node is marked as shared, which must be (*) done while there are no
 *  pending mutations.  New mutating operations are blocked if a mark is
 *  pending, and when existing mutating operations are completed the mark is
 *  made.
 *  <em>* - It would be less disruptive if we immediately marked the root as
 *  shared, and then waited for pending operations that might not have seen the
 *  mark without blocking new mutations.  This could result in imbalance being
 *  frozen into the shared portion of the tree, though.  To minimize the
 *  problem we perform the mark and reenable mutation on whichever thread
 *  notices that the entry count has become zero, to reduce context switches on
 *  the critical path.</em>
 *
 *  <p>The same multi-cache line data structure required for efficiently
 *  tracking the entry and exit for mutating operations is used to maintain the
 *  current size of the tree.  This means that the size can be computed by
 *  quiescing as for a clone, but without doing any marking.
 *
 *  <p>Range queries such as higherKey are not amenable to the optimistic
 *  hand-over-hand locking scheme used for exact searches, so they are
 *  implemented with pessimistic concurrency control.  Mutation can be
 *  considered to acquire a lock on the map in Intention-eXclusive mode, range
 *  queries, size(), and root marking acquire the lock in Shared mode.
 *
 *  @author Nathan Bronson
 */
@SuppressWarnings("ALL")
public class GridOffHeapSnapTreeMap<K extends GridOffHeapSmartPointer,V extends GridOffHeapSmartPointer>
    extends AbstractMap<K,V> implements ConcurrentNavigableMap<K, V>, Cloneable, Closeable {
    /** This is a special value that indicates that an optimistic read failed. */
    private static final GridOffHeapSmartPointer SpecialRetry = new GridOffHeapSmartPointer() {
        @Override public long pointer() {
            throw new IllegalStateException();
        }

        @Override public void incrementRefCount() {
            throw new IllegalStateException();
        }

        @Override public void decrementRefCount() {
            throw new IllegalStateException();
        }
    };

    /** The number of spins before yielding. */
    private static final int SpinCount = Integer.parseInt(System.getProperty("snaptree.spin", "100"));

    /** The number of yields before blocking. */
    private static final int YieldCount = Integer.parseInt(System.getProperty("snaptree.yield", "0"));

    /** We encode directions as characters. */
    private static final char Left = 'L';

    /** We encode directions as characters. */
    private static final char Right = 'R';

    /**
     *  An <tt>OVL</tt> is a version number and lock used for optimistic
     *  concurrent control of some program invariant.  If  {@link #isShrinking}
     *  then the protected invariant is changing.  If two reads of an OVL are
     *  performed that both see the same non-changing value, the reader may
     *  conclude that no changes to the protected invariant occurred between
     *  the two reads.  The special value UnlinkedOVL is not changing, and is
     *  guaranteed to not result from a normal sequence of beginChange and
     *  endChange operations.
     *  <p>
     *  For convenience <tt>endChange(ovl) == endChange(beginChange(ovl))</tt>.
     */
    private static long beginChange(long ovl) {
        return ovl | 1;
    }

    /**
     * @param ovl OVL.
     * @return OVL.
     */
    private static long endChange(long ovl) {
        return (ovl | 3) + 1;
    }

    /** */
    private static final long UnlinkedOVL = 2;

    /**
     * @param ovl OVL.
     * @return OVL.
     */
    private static boolean isShrinking(long ovl) {
        return (ovl & 1) != 0;
    }

    /**
     * @param ovl OVL.
     * @return OVL.
     */
    private static boolean isUnlinked(long ovl) {
        return (ovl & 2) != 0;
    }

    /**
     * @param ovl OVL.
     * @return OVL.
     */
    private static boolean isShrinkingOrUnlinked(long ovl) {
        return (ovl & 3) != 0L;
    }

    /** Scale of long. */
    static final int scale = 8;

    /** */
    static final int KEY = 0 * scale;

    /** */
    static final int V_OPT = 1 * scale;

    /** */
    static final int LEFT = 2 * scale;

    /** */
    static final int RIGHT = 3 * scale;

    /** */
    static final int SHRINK_OVL = 4 * scale;

    /** */
    static final int PARENT = 5 * scale;

    /** */
    static final int HEIGHT = 6 * scale;

    /** */
    static final int LONGS_IN_NODE = 7;

    /** */
    static final int NODE_SIZE = LONGS_IN_NODE * scale;

    /**
     * Resizable array of long values.
     */
    private static class LongArray {
        /** */
        private long[] arr;

        /** */
        private int idx = 0;

        /**
         * Add element to this array.
         * @param x Value.
         */
        public void add(long x) {
            if (arr == null)
                arr = new long[4];
            else if (arr.length == idx)
                arr = Arrays.copyOf(arr, arr.length * 2);

            arr[idx++] = x;
        }
    }

    /**
     * @param key Key.
     * @param height Height.
     * @param vOpt Value.
     * @param parent Parent.
     * @param left Left.
     * @param right Right.
     * @return New node.
     */
    private long newNode(final K key, final int height, final V vOpt, final long parent, long left, long right) {
        long ptr = mem.allocate(NODE_SIZE);

        vOpt0(ptr);

        assert (vOpt != null && key != null) || (key == vOpt);

        key(ptr, key);
        height(ptr, height);
        vOpt(ptr, vOpt);
        parent(ptr, parent);
        shrinkOVL(ptr, 0);
        left(ptr, left);
        right(ptr, right);

        return ptr;
    }

    /**
     * @param node Node.
     * @return Key pointer.
     */
    protected long keyPtr(long node) {
        return mem.readLongVolatile(node + KEY);
    }

    /**
     * @param ptr Node pointer.
     * @return Key.
     */
    private K key(long ptr) {
        long resPtr = mem.readLongVolatile(ptr + KEY);

        if (resPtr == 0) {
            return null;
        }

        return keyFactory.createPointer(resPtr);
    }

    /**
     * @param ptr Node pointer.
     * @return Value.
     */
    private V vOpt(long ptr) {
        long resPtr = mem.readLongVolatile(ptr + V_OPT);

        if (resPtr == 0)
            return null;

        return valFactory.createPointer(resPtr);
    }

    /**
     * @param ptr Node pointer.
     * @return {@code true} If value is null;
     */
    private boolean vOptIsNull(long ptr) {
        return mem.readLongVolatile(ptr + V_OPT) == 0;
    }

    /**
     * @param ptr Node pointer.
     * @return Right child.
     */
    long right(long ptr) {
        return mem.readLongVolatile(ptr + RIGHT);
    }

    /**
     * @param ptr Node pointer.
     * @return Left child.
     */
    long left(long ptr) {
        return mem.readLongVolatile(ptr + LEFT);
    }

    /**
     * @param ptr Node pointer.
     * @param nodePtr Right child's pointer.
     */
    protected void right(long ptr, long nodePtr) {
        assert nodePtr >= 0;

        mem.writeLongVolatile(ptr + RIGHT, nodePtr);
    }

    /**
     * @param ptr Node pointer.
     * @param nodePtr Left child's pointer.
     */
    private void left(long ptr, long nodePtr) {
        assert nodePtr >= 0;

        mem.writeLongVolatile(ptr + LEFT, nodePtr);
    }

    /**
     * @param ptr Node pointer.
     * @return Parents pointer.
     */
    private long parent(long ptr) {
        return mem.readLongVolatile(ptr + PARENT);
    }

    /**
     * @param ptr Node pointer.
     * @param nodePtr Parents pointer.
     */
    private void parent(long ptr, long nodePtr) {
        assert nodePtr >= 0;

        mem.writeLongVolatile(ptr + PARENT, nodePtr);
    }

    /**
     * Reads link from given node pointer to next when they are getting dealloctated.
     *
     * @param addr Node pointer.
     * @return Next node pointer.
     */
    private long readLink(long addr) {
        long ptr = -mem.readLongVolatile(addr + PARENT);

        assert ptr >= 0;

        return ptr;
    }

    /**
     * Writes link to given pointer to this pointer.
     *
     * @param addr Node pointer.
     * @param val Next node pointer.
     */
    private void writeLink(long addr, long val) {
        assert val > 0;

        mem.writeLongVolatile(addr + PARENT, -val);

////    !!! The same code but with assertions. Don't remove!!!
//
//        val = -val;
//        addr += PARENT;
//
//        long old;
//
//        do {
//            old = mem.readLongVolatile(addr);
//
//            D.hangIfStopped();
//
//            if (old == val) {
//                assert old == -Long.MAX_VALUE : old;
//
//                return;
//            }
//
//            if (prev == 0) {
//                assert old >= 0 || // First time added.
//                        old == -Long.MAX_VALUE : // Added as tail to another queue.
//                        (addr - PARENT) + " " + old + D.dumpWithStop();
//            }
//            else
//                assert -prev == old : old + " " + prev + D.dumpWithStop();
//        }
//        while (!mem.casLong(addr, old, val));
    }

    /**
     * Lazy copy node and mark children shared.
     *
     * @param ptr Node pointer.
     * @param newParent New parent pointer.
     * @param unlinked Array for unlinked values.
     * @return Copy of node.
     */
    private long lazyCopy(long ptr, long newParent, LongArray unlinked) {
        assert ptr > 0;
        assert (isShared(ptr));
        assert (!isShrinkingOrUnlinked(shrinkOVL(ptr)));

        if (snapshots.isEmpty()) { // Unshare node if there are no snapshots.
            parent(ptr, newParent);

            return ptr;
        }

        long n = mem.allocate(NODE_SIZE);

        mem.copyMemory(ptr, n, NODE_SIZE);

        markShared(left(n));
        markShared(right(n));

        key(n).incrementRefCount();

        GridOffHeapSmartPointer val = vOpt(n);

        if (val != null)
            val.incrementRefCount();

        parent(n, newParent);

        // Start recycling
        unlinked.add(-ptr); // Minus to show that node should be added to older snapshot.

        return n;
    }

    /**
     * @param ptr Node pointer.
     * @return OVL for given node.
     */
    private long shrinkOVL(long ptr) {
        return mem.readLongVolatile(ptr + SHRINK_OVL);
    }

    /**
     * @param ptr Node pointer.
     * @return Node height.
     */
    private int height(long ptr) {
        return ptr == 0 ? 0 : (int)mem.readLongVolatile(ptr + HEIGHT);
    }

    /**
     * @param ptr Node pointer.
     * @param shrinkOVL OVL.
     */
    private void shrinkOVL(long ptr, long shrinkOVL) {
        mem.writeLongVolatile(ptr + SHRINK_OVL, shrinkOVL);
    }

    /**
     * Reset value.
     *
     * @param ptr Node pointer.
     */
    private void vOpt0(long ptr) {
        mem.writeLongVolatile(ptr + V_OPT, 0);
    }

    /**
     * @param ptr Node pointer.
     * @param newValue New value.
     */
    private void vOpt(long ptr, GridOffHeapSmartPointer newValue) {
        GridOffHeapSmartPointer old = vOpt(ptr);

        long newValPtr = 0;

        if (newValue != null) {
            newValue.incrementRefCount();

            newValPtr = newValue.pointer();
        }

        assert newValPtr >= 0;

        mem.writeLongVolatile(ptr + V_OPT, newValPtr);

        if (old != null)
            old.decrementRefCount();
    }

    /**
     * @param ptr Node pointer.
     * @param key Key.
     */
    protected void key(long ptr, K key) {
        long keyPtr = 0;

        if (key != null) {
            key.incrementRefCount();

            keyPtr = key.pointer();
        }

        mem.writeLongVolatile(ptr + KEY, keyPtr);
    }

    /**
     * @param ptr Node pointer.
     * @param h Height.
     */
    private void height(long ptr, int h) {
        mem.writeLongVolatile(ptr + HEIGHT, h);
    }

    /**
     * @param ptr Node pointer.
     * @param dir Child direction.
     * @return Child.
     */
    private long child(long ptr, char dir) {
        return dir == Left ? left(ptr) : right(ptr);
    }

    /**
     * @param ptr Node pointer.
     * @param dir Child direction.
     * @param node Child pointer.
     */
    private void setChild(long ptr, char dir, long node) {
        if (dir == Left) {
            left(ptr, node);
        }
        else {
            right(ptr, node);
        }
    }

    /**
     * @param node Node pointer.
     * @return If node is shared.
     */
    private boolean isShared(final long node) {
        assert node >= 0;

        // Need to check isUnlinked because node on unlink will have negative parent,
        // but it does not mean that it is shared.
        return node != 0 && parent(node) <= 0 && !isUnlinked(shrinkOVL(node));
    }

    /**
     * @param node Node pointer.
     * @return Given node.
     */
    private long markShared(final long node) {
        assert node >= 0;

        if (node != 0 && parent(node) > 0)
            parent(node, 0);

        return node;
    }

    /**
     * @param ptr Node pointer.
     * @param unlinked Array for unlinked nodes.
     * @return Unshared left child.
     */
    private long unsharedLeft(long ptr, LongArray unlinked) {
        final long cl = left(ptr);

        if (!isShared(cl)) {
            return cl;
        }
        else {
            lazyCopyChildren(ptr, unlinked);

            return left(ptr);
        }
    }

    /**
     * @param ptr Node pointer.
     * @param unlinked Array for unlinked nodes.
     * @return Unshared right child.
     */
    private long unsharedRight(long ptr, LongArray unlinked) {
        final long cr = right(ptr);

        if (!isShared(cr)) {
            return cr;
        }
        else {
            lazyCopyChildren(ptr, unlinked);

            return right(ptr);
        }
    }

    /**
     * @param ptr Node pointer.
     * @param dir Direction.
     * @param unlinked Array for unlinked nodes.
     * @return Unshared right child.
     */
    private long unsharedChild(long ptr, final char dir, LongArray unlinked) {
        return dir == Left ? unsharedLeft(ptr, unlinked) : unsharedRight(ptr, unlinked);
    }

    /**
     * @param ptr Node pointer.
     * @param unlinked Array for unlinked nodes.
     */
    private void lazyCopyChildren(long ptr, LongArray unlinked) {
        KeyLock.Lock l = lock.lock(ptr);

        try {
            final long cl = left(ptr);

            if (isShared(cl)) {
                left(ptr, lazyCopy(cl, ptr, unlinked));
            }

            final long cr = right(ptr);

            if (isShared(cr)) {
                right(ptr, lazyCopy(cr, ptr, unlinked));
            }
        }
        finally {
            if (l != null)
                l.unlock();
        }
    }

    /**
     * @param ptr Node pointer.
     * @param ovl Known OVL.
     */
    private void waitUntilShrinkCompleted(long ptr, final long ovl) {
        if (isUnlinked(ovl) || !isShrinking(ovl)) {
            return;
        }

        for (int tries = 0; tries < SpinCount; ++tries) {
            if (shrinkOVL(ptr) != ovl) {
                return;
            }
        }

        for (int tries = 0; tries < YieldCount; ++tries) {
            Thread.yield();

            if (shrinkOVL(ptr) != ovl) {
                return;
            }
        }

        // spin and yield failed, use the nuclear option
        // we can't have gotten the lock unless the shrink was over
        KeyLock.Lock l = lock.lock(ptr);

        if (l != null)
            l.unlock();

        assert(shrinkOVL(ptr) != ovl) : ptr + " " + ovl;
    }

    /**
     * @param ptr Node pointer.
     * @return Validated height.
     */
    private int validatedHeight(long ptr) {
        final int hL = left(ptr) == 0 ? 0 : validatedHeight(left(ptr));
        final int hR = right(ptr) == 0 ? 0 : validatedHeight(right(ptr));

        assert(Math.abs(hL - hR) <= 1);
        final int h = 1 + Math.max(hL, hR);

        int resH = height(ptr);

        assert(h == resH);

        return resH;
    }

    /**
     * @param root Root node pointer.
     * @param fromCmp Lower bound.
     * @param fromIncl Include lower bound.
     * @param toCmp Upper bound.
     * @param toIncl Include upper bound.
     * @return Size.
     */
    private int computeFrozenSize(long root, Comparable<? super K> fromCmp, boolean fromIncl,
        final Comparable<? super K> toCmp, final boolean toIncl) {
        int result = 0;

        while (true) {
            if (root == 0) {
                return result;
            }

            if (fromCmp != null) {
                final int c = fromCmp.compareTo(key(root));

                if (c > 0 || (c == 0 && !fromIncl)) {
                    // all matching nodes are on the right side
                    root = right(root);

                    continue;
                }
            }

            if (toCmp != null) {
                final int c = toCmp.compareTo(key(root));

                if (c < 0 || (c == 0 && !toIncl)) {
                    // all matching nodes are on the left side
                    root = left(root);

                    continue;
                }
            }

            // Current node matches.  Nodes on left no longer need toCmp, nodes
            // on right no longer need fromCmp.
            if (!vOptIsNull(root)) {
                ++result;
            }

            result += computeFrozenSize(left(root), fromCmp, fromIncl, null, false);

            fromCmp = null;

            root = right(root);
        }
    }

    /**
     * Entry of tree.
     */
    private class Entree implements Map.Entry<K,V> {
        /** */
        private final long ptr;

        /**
         * @param ptr Pointer.
         */
        private Entree(long ptr) {
            this.ptr = ptr;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key(ptr);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public V getValue() {
            return vOpt(ptr);
        }

        /** {@inheritDoc} */
        @Override public GridOffHeapSmartPointer setValue(final GridOffHeapSmartPointer v) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }

            final Map.Entry rhs = (Map.Entry)o;

            return key(ptr).equals(rhs.getKey()) && getValue().equals(rhs.getValue());
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (getKey() == null ? 0 : getKey().hashCode()) ^ (getValue() == null ? 0 : getValue().hashCode());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return key(ptr) + "=" + getValue();
        }
    }

    /**
     * Recycle queue for memory deallocation.
     */
    private class RecycleQueue implements GridUnsafeCompoundMemory {
        /** */
        private volatile long tail;

        /** */
        protected final AtomicLong head = new AtomicLong(Long.MAX_VALUE);

        /** */
        protected final AtomicLong size = new AtomicLong();

        /**
         * @return Size.
         */
        long size() {
            return size.get();
        }

        /**
         * @param node Node pointer.
         * @return {@code true} If operation succeeded.
         */
        boolean add(long node) {
            assert node > 0;

            return add(node, node, 1);
        }

        /** {@inheritDoc} */
        @Override public void merge(GridUnsafeCompoundMemory compound) {
            boolean res = add((RecycleQueue)compound);

            assert res;
        }

        /**
         * @param q Recycle queue.
         * @return {@code true} If operation succeeded.
         */
        public boolean add(RecycleQueue q) {
            assert !q.isEmpty();

            long node = q.head.get();
            long tail = q.tail;
            long size = q.size();

            assert node > 0;
            assert tail > 0;
            assert size > 0;

            return add(node, tail, size);
        }

        /**
         * @param node Head node pointer.
         * @param tail Tail node pointer.
         * @param size Size of the chain.
         * @return {@code true} If succeeded.
         */
        protected boolean add(long node, long tail, long size) {
            for (;;) {
                final long h = head.get();

                assert h > 0 : h;

                writeLink(tail, h);// If h == Long.MAX_VALUE we still need to write it to tail.

                if (head.compareAndSet(h, node)) {
                    if (h == Long.MAX_VALUE)
                        this.tail = tail;

                    this.size.addAndGet(size);

                    return true;
                }
            }
        }

        /**
         * @return {@code true} If empty.
         */
        boolean isEmpty() {
            return head.get() == Long.MAX_VALUE;
        }

        /** {@inheritDoc} */
        @Override public void deallocate() {
            long h = head.get();

            while (h != Long.MAX_VALUE) {
                assert h > 0 : h;

                GridOffHeapSmartPointer k = key(h);

                if (k != null) {
                    k.decrementRefCount();

                    GridOffHeapSmartPointer v = vOpt(h);

                    if (v != null)
                        v.decrementRefCount();
                }

                long prev = h;

                h = readLink(h);

                mem.release(prev, NODE_SIZE);
            }
        }
    }

    /**
     * Stoppable recycle queue.
     */
    private class StoppableRecycleQueue extends RecycleQueue {
        /** */
        protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        /** */
        private final AtomicBoolean stopped = new AtomicBoolean(false);

        /** {@inheritDoc} */
        @Override public boolean add(long node) {
            ReentrantReadWriteLock.ReadLock l =  lock.readLock();

            if (!l.tryLock())
                return false;

            try {
                return super.add(node);
            }
            finally {
                l.unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean add(RecycleQueue que) {
            ReentrantReadWriteLock.ReadLock l =  lock.readLock();

            if (!l.tryLock())
                return false;

            try {
                return super.add(que);
            }
            finally {
                l.unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void merge(GridUnsafeCompoundMemory compound) {
            boolean res = super.add((RecycleQueue)compound);

            assert res;
        }

        /**
         * @return {@code true} If we stopped this queue.
         */
        public boolean stop() {
            if (stopped.compareAndSet(false, true)) {
                lock.writeLock().lock();

                return true;
            }

            return false;
        }

        /**
         * @return {@code true} If this queue is stopped..
         */
        public boolean isStopped() {
            return stopped.get();
        }
    }

    /**
     * @return New root holder.
     */
    private long rootHolder() {
        return newNode(null, 1, null, 0L, 0L, 0L);
    }

    /**
     * @param rootHolderSnapshot Snapshot of current root holder.
     * @return Copy of given root holder.
     */
    private long rootHolder(final long rootHolderSnapshot) {
        return newNode(null, 1 + height(rootHolderSnapshot), null, 0L, 0L, right(rootHolderSnapshot));
    }

    /** */
    private final Comparator<? super K> comparator;

    /** */
    private transient volatile long holderRef;

    /** */
    private final GridOffHeapSmartPointerFactory<K> keyFactory;

    /** */
    private final GridOffHeapSmartPointerFactory<V> valFactory;

    /** */
    protected final GridUnsafeMemory mem;

    /** */
    protected final GridUnsafeGuard guard;

    /** */
    private final KeyLock lock = new KeyLock();

    /** */
    private final AtomicLong snapshotsCnt = new AtomicLong();

    /** */
    private final ConcurrentSkipListMap<Long, GridOffHeapSnapTreeMap> snapshots =
        new ConcurrentSkipListMap<Long, GridOffHeapSnapTreeMap>();

    /** */
    private Long snapshotId = 0L;

    /** Recycle queue for this snapshot. */
    private volatile StoppableRecycleQueue recycleBin;

    /**
     * @param keyFactory Key factory.
     * @param valFactory Value factory.
     * @param mem Unsafe memory.
     * @param guard Guard.
     */
    public GridOffHeapSnapTreeMap(GridOffHeapSmartPointerFactory keyFactory, GridOffHeapSmartPointerFactory valFactory,
        GridUnsafeMemory mem, GridUnsafeGuard guard) {
        this(keyFactory, valFactory, mem, guard, null);
    }

    /**
     * @param keyFactory Key factory.
     * @param valFactory Value factory.
     * @param mem Unsafe memory.
     * @param guard Guard.
     * @param comparator Comparator.
     */
    public GridOffHeapSnapTreeMap(GridOffHeapSmartPointerFactory keyFactory, GridOffHeapSmartPointerFactory valFactory,
        GridUnsafeMemory mem, GridUnsafeGuard guard, Comparator<? super K> comparator) {
        assert keyFactory != null;
        assert valFactory != null;
        assert mem != null;
        assert guard != null;

        this.comparator = comparator;
        this.keyFactory = keyFactory;
        this.valFactory = valFactory;
        this.mem = mem;
        this.guard = guard;
        this.holderRef = rootHolder();
    }

    /**
     * Closes tree map and reclaims memory.
     */
    public void close() {
        RecycleQueue q;

        if (snapshotId.longValue() == 0) {
            assert recycleBin == null;

            q = new RecycleQueue();

            deallocateSubTree(root(), q);
        }
        else {
            GridOffHeapSnapTreeMap s = snapshots.remove(snapshotId);

            assert s == this;

            q = recycleBin;

            boolean res = recycleBin.stop();

            assert res;
        }

        mem.release(holderRef, NODE_SIZE);

        if (!q.isEmpty())
            doDeallocateSnapshot(q);
    }

    /**
     * Deallocates sub-tree under given node.
     *
     * @param node Node pointer.
     * @param que Recycle queue.
     */
    private void deallocateSubTree(long node, RecycleQueue que) {
        if (node == 0)
            return;

        deallocateSubTree(left(node), que);
        deallocateSubTree(right(node), que);

        que.add(node);
    }

    /**
     * @return Clones this map and takes data snapshot.
     */
    @SuppressWarnings("unchecked")
    @Override public GridOffHeapSnapTreeMap<K,V> clone() {
        final GridOffHeapSnapTreeMap copy;
        try {
            copy = (GridOffHeapSnapTreeMap) super.clone();
        } catch (final CloneNotSupportedException xx) {
            throw new InternalError();
        }
        assert(copy.comparator == comparator);

        copy.holderRef = rootHolder(holderRef);
        markShared(root());

        copy.size = new AtomicInteger(size());
        copy.recycleBin = new StoppableRecycleQueue();

        copy.snapshotId = snapshotsCnt.decrementAndGet(); // To put in descending order.

        snapshots.put(copy.snapshotId, copy);

        return copy;
    }

    /**
     * @return Root node.
     */
    long root() {
        return right(holderRef);
    }

    /**
     * Counts all tree nodes.
     *
     * @param nonNull Only not routing nodes.
     * @return Count.
     */
    long nodes(boolean nonNull) {
        return nodes(root(), nonNull);
    }

    /**
     * Counts all subtree nodes.
     *
     * @param node Root of subtree.
     * @param nonNull Only not routing nodes.
     * @return Count.
     */
    private long nodes(long node, boolean nonNull) {
        if (node == 0)
            return 0;

        return (nonNull && vOptIsNull(node) ? 0 : 1) + nodes(left(node), nonNull) + nodes(right(node), nonNull);
    }

    /**
     * @return String representation of structure.
     */
    String print() {
        long root = root();

        if (root == 0)
            return "Empty tree";

        ArrayList<SB> s = new ArrayList<SB>(height(root) + 1);

        print(root, s, 0, 0);

        SB res = new SB();

        for (SB sb : s)
            res.a(sb).a('\n');

        return '\n' + res.toString();
    }

    /**
     * @param node Node.
     * @param s String builders.
     * @param level Level.
     * @param offset Offset.
     * @return Length.
     */
    private int print(long node, ArrayList<SB> s, int level, int offset) {
        if (node == 0)
            return s.get(level - 1).length();

        SB sb = s.size() <= level ? null : s.get(level);

        if (sb == null) {
            sb = new SB();

            s.add(level, sb);
        }

        int o = Math.max(print(left(node), s, level + 1, offset), offset);

        String v = print0(node);

        while(sb.length() < o)
            sb.a(' ');

        sb.a(v);

        return print(right(node), s, level + 1, o + v.length());
    }

    /**
     * Print node.
     *
     * @param node Node.
     * @return Debug string representation.
     */
    private String print0(long node) {
        if (key(node) == null)
            return "[" + node + "]";

        return "[(" + node + ":" + shrinkOVL(node) + " " + parent(node) + ") " + key(node) + " = " + vOpt(node) + " ]";
    }

    /**
     * Validates tree structure.
     */
    void validate() {
        long res = validate(root());

        assert res == 0;
    }

    /**
     * Validates subtree structure.
     *
     * @param node Subtree root.
     * @return Node at wrong position.
     */
    private long validate(long node) {
        if (node == 0)
            return 0;

        long left = left(node);

        if (left != 0) {
            if (comparable(key(left)).compareTo(key(node)) >= 0)
                return node;

            long res = validate(left);

            if (res != 0)
                return res;
        }

        long right = right(node);

        if (right != 0) {
            if (comparable(key(right)).compareTo(key(node)) <= 0)
                return node;

            long res = validate(right);

            if (res != 0)
                return res;
        }

        return 0;
    }

    /** */
    private AtomicInteger size = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public int size() {
        return size.get();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        // removed-but-not-unlinked nodes cannot be leaves, so if the tree is
        // truly empty then the root holder has no right child
        return root() == 0;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Comparator<? super K> comparator() {
        return comparator;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(final Object key) {
        return getImpl((K)key) != null;
    }

    /** {@inheritDoc} */
    @Override public V get(final Object key) {
        return (V)getImpl((K)key);
    }

    /**
     * @param key Key.
     * @return Comparable over given key.
     */
    @SuppressWarnings("unchecked")
    protected Comparable<? super K> comparable(final Object key) {
        if (key == null) {
            throw new NullPointerException();
        }

        if (comparator == null) {
            return (Comparable<? super K>)key;
        }

        return new Comparable<K>() {
            final Comparator<? super K> _cmp = comparator;

            @SuppressWarnings("unchecked")
            public int compareTo(final K rhs) { return _cmp.compare((K)key, rhs); }
        };
    }

    /**
     * Returns either a value or SpecialNull, if present, or null, if absent.
     */
    private GridOffHeapSmartPointer getImpl(final K key) {
        LongArray unlinked = new LongArray();

        try {
            return doGet(key, unlinked);
        }
        finally {
            deallocate(unlinked);
        }
    }

    /**
     * @param key Key.
     * @param unlinked Array for unlinked nodes.
     * @return Found pointer.
     */
    private GridOffHeapSmartPointer doGet(final K key, LongArray unlinked) {
        final Comparable<? super K> k = comparable(key);

        while (true) {
            final long right = unsharedRight(holderRef, unlinked);

            if (right == 0) {
                return null;
            }
            else {
                final int rightCmp = k.compareTo(key(right));

                if (rightCmp == 0) {
                    // who cares how we got here
                    return vOpt(right);
                }

                final long ovl = shrinkOVL(right);

                if (isShrinkingOrUnlinked(ovl)) {
                    waitUntilShrinkCompleted(right, ovl);
                    // RETRY
                }
                else if (right == right(holderRef)) {
                    // the reread of .right is the one protected by our read of ovl
                    GridOffHeapSmartPointer vo = attemptGet(k, right, (rightCmp < 0 ? Left : Right), ovl, unlinked);

                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    /**
     * @param k Key.
     * @param node Node pointer.
     * @param dirToC Direction.
     * @param nodeOVL OVL.
     * @param unlinked Array for unlinked nodes.
     * @return Found value.
     */
    private GridOffHeapSmartPointer attemptGet(final Comparable<? super K> k, final long node, final char dirToC,
        final long nodeOVL, LongArray unlinked) {
        while (true) {
            final long child = unsharedChild(node, dirToC, unlinked);

            if (child == 0) {
                if (isOutdatedOVL(node, nodeOVL)) {
                    return SpecialRetry;
                }

                // Note is not present.  Read of node.child occurred while
                // parent.child was valid, so we were not affected by any
                // shrinks.
                return null;
            }
            else {
                final int childCmp = k.compareTo(key(child));

                if (childCmp == 0) {
                    // how we got here is irrelevant
                    return vOpt(child);
                }

                // child is non-null
                final long childOVL = shrinkOVL(child);

                if (isShrinkingOrUnlinked(childOVL)) {
                    waitUntilShrinkCompleted(child, childOVL);

                    if (isOutdatedOVL(node, nodeOVL)) {
                        return SpecialRetry;
                    }
                    // else RETRY
                }
                else if (child != child(node, dirToC)) {
                    // this .child is the one that is protected by childOVL
                    if (isOutdatedOVL(node, nodeOVL)) {
                        return SpecialRetry;
                    }
                    // else RETRY
                }
                else {
                    if (isOutdatedOVL(node, nodeOVL)) {
                        return SpecialRetry;
                    }

                    // At this point we know that the traversal our parent took
                    // to get to node is still valid.  The recursive
                    // implementation will validate the traversal from node to
                    // child, so just prior to the nodeOVL validation both
                    // traversals were definitely okay.  This means that we are
                    // no longer vulnerable to node shrinks, and we don't need
                    // to validate nodeOVL any more.
                    GridOffHeapSmartPointer vo = attemptGet(k, child, (childCmp < 0 ? Left : Right), childOVL, unlinked);

                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public K firstKey() {
        return extremeKeyOrThrow(Left);
    }

    /** {@inheritDoc} */
    @Override public Map.Entry<K,V> firstEntry() {
        return (SimpleImmutableEntry<K,V>)extreme(false, Left);
    }

    /** {@inheritDoc} */
    @Override public K lastKey() {
        return extremeKeyOrThrow(Right);
    }

    /** {@inheritDoc} */
    @Override public Map.Entry<K,V> lastEntry() {
        return (SimpleImmutableEntry<K,V>) extreme(false, Right);
    }

    /**
     * @param dir Direction.
     * @return Extreme key.
     */
    private K extremeKeyOrThrow(final char dir) {
        final K k = (K) extreme(true, dir);

        if (k == null) {
            throw new NoSuchElementException();
        }

        return k;
    }

    /**
     *  Returns a key if returnKey is true, a SimpleImmutableEntry otherwise.
     *  Returns null if none exists.
     */
    private Object extreme(final boolean returnKey, final char dir) {
        while (true) {
            final long right = right(holderRef);

            if (right == 0) {
                return null;
            }
            else {
                final long ovl = shrinkOVL(right);
                if (isShrinkingOrUnlinked(ovl)) {
                    waitUntilShrinkCompleted(right, ovl);
                    // RETRY
                }
                else if (right == right(holderRef)) {
                    // the reread of .right is the one protected by our read of ovl
                    final Object vo = attemptExtreme(returnKey, dir, right, ovl);

                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    /**
     * @param returnKey If we need to return key.
     * @param dir Direction.
     * @param node Node pointer.
     * @param nodeOVL OVL.
     * @return Some argument dependant result.
     */
    private Object attemptExtreme(final boolean returnKey,
                                  final char dir,
                                  final long node,
                                  final long nodeOVL) {
        while (true) {
            final long child = child(node, dir);

            if (child == 0) {
                // Read of the value must be protected by the OVL, because we
                // must linearize against another thread that inserts a new min
                // key and then changes this key's value
                V vo = vOpt(node);

                if (isOutdatedOVL(node, nodeOVL)) {
                    return SpecialRetry;
                }

                assert(vo != null);

                return returnKey ? key(node) : new SimpleImmutableEntry<K,V>(key(node), vo);
            }
            else {
                // child is non-null
                final long childOVL = shrinkOVL(child);

                if (isShrinkingOrUnlinked(childOVL)) {
                    waitUntilShrinkCompleted(child, childOVL);

                    if (isOutdatedOVL(node, nodeOVL)) {
                        return SpecialRetry;
                    }
                    // else RETRY
                }
                else if (child != child(node, dir)) {
                    // this .child is the one that is protected by childOVL
                    if (isOutdatedOVL(node, nodeOVL)) {
                        return SpecialRetry;
                    }
                    // else RETRY
                }
                else {
                    if (isOutdatedOVL(node, nodeOVL)) {
                        return SpecialRetry;
                    }

                    final Object vo = attemptExtreme(returnKey, dir, child, childOVL);

                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public K lowerKey(final K key) {
        return (K)boundedExtreme(null, false, comparable(key), false, true, Right);
    }

    /** {@inheritDoc} */
    @Override public K floorKey(final K key) {
        return (K)boundedExtreme(null, false, comparable(key), true, true, Right);
    }

    /** {@inheritDoc} */
    @Override public K ceilingKey(final K key) {
        return (K)boundedExtreme(comparable(key), true, null, false, true, Left);
    }

    /** {@inheritDoc} */
    @Override public K higherKey(final K key) {
        return (K)boundedExtreme(comparable(key), false, null, false, true, Left);
    }

    /** {@inheritDoc} */
    @Override public Entry<K,V> lowerEntry(final K key) {
        return (Entry<K,V>) boundedExtreme(null, false, comparable(key), false, false, Right);
    }

    /** {@inheritDoc} */
    @Override public Entry<K,V> floorEntry(final K key) {
        return (Entry<K,V>) boundedExtreme(null, false, comparable(key), true, false, Right);
    }

    /** {@inheritDoc} */
    @Override public Entry<K,V> ceilingEntry(final K key) {
        return (Entry<K,V>) boundedExtreme(comparable(key), true, null, false, false, Left);
    }

    /** {@inheritDoc} */
    @Override public Entry<K,V> higherEntry(final K key) {
        return (Entry<K,V>) boundedExtreme(comparable(key), false, null, false, false, Left);
    }

    /** Returns null if none exists. */
    private K boundedExtremeKeyOrThrow(final Comparable<? super K> minCmp, final boolean minIncl,
        final Comparable<? super K> maxCmp, final boolean maxIncl, final char dir) {
        final K k = (K)boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, true, dir);

        if (k == null) {
            throw new NoSuchElementException();
        }

        return k;
    }

    /** Returns null if none exists. */
    @SuppressWarnings("unchecked")
    private Object boundedExtreme(final Comparable<? super K> minCmp, final boolean minIncl,
        final Comparable<? super K> maxCmp, final boolean maxIncl, final boolean returnKey, final char dir) {
        K resultKey;
        Object result;

        if ((dir == Left && minCmp == null) || (dir == Right && maxCmp == null)) {
            // no bound in the extreme direction, so use the concurrent search
            result = extreme(returnKey, dir);

            if (result == null) {
                return null;
            }

            resultKey = returnKey ? (K)result : ((SimpleImmutableEntry<K,V>) result).getKey();
        }
        else {
            long holder = holderRef;

            final long node = (dir == Left) ? boundedMin(right(holder), minCmp, minIncl) :
                    boundedMax(right(holder), maxCmp, maxIncl);

            if (node == 0) {
                return null;
            }

            resultKey = key(node);

            if (returnKey) {
                result = resultKey;
            }
            else {
                // we must copy the node
                result = new SimpleImmutableEntry<K,V>(key(node), vOpt(node));
            }
        }

        if (dir == Left && maxCmp != null) {
            final int c = maxCmp.compareTo(resultKey);

            if (c < 0 || (c == 0 && !maxIncl)) {
                return null;
            }
        }

        if (dir == Right && minCmp != null) {
            final int c = minCmp.compareTo(resultKey);

            if (c > 0 || (c == 0 && !minIncl)) {
                return null;
            }
        }

        return result;
    }

    /**
     * @param node Node.
     * @param minCmp Lower bound.
     * @param minIncl Include min bound.
     * @return Bounded min.
     */
    private long boundedMin(long node, final Comparable<? super K> minCmp, final boolean minIncl) {
        while (node != 0) {
            final int c = minCmp.compareTo(key(node));

            if (c < 0) {
                // there may be a matching node on the left branch
                final long z = boundedMin(left(node), minCmp, minIncl);

                if (z != 0) {
                    return z;
                }
            }

            if (c < 0 || (c == 0 && minIncl)) {
                // this node is a candidate, is it actually present?
                if (!vOptIsNull(node)) {
                    return node;
                }
            }

            // the matching node is on the right branch if it is present
            node = right(node);
        }

        return 0L;
    }

    /**
     * @param node Node.
     * @param maxCmp Upper bound.
     * @param maxIncl Include upper bound.
     * @return Bounded max.
     */
    private long boundedMax(long node, final Comparable<? super K> maxCmp, final boolean maxIncl) {
        while (node != 0) {
            final int c = maxCmp.compareTo(key(node));

            if (c > 0) {
                // there may be a matching node on the right branch
                final long z = boundedMax(right(node), maxCmp, maxIncl);

                if (z != 0) {
                    return z;
                }
            }

            if (c > 0 || (c == 0 && maxIncl)) {
                // this node is a candidate, is it actually present?
                if (!vOptIsNull(node)) {
                    return node;
                }
            }

            // the matching node is on the left branch if it is present
            node = left(node);
        }

        return 0;
    }

    /** */
    private static final int UpdateAlways = 0;

    /** */
    private static final int UpdateIfAbsent = 1;

    /** */
    private static final int UpdateIfPresent = 2;

    /** */
    private static final int UpdateIfEq = 3;

    /**
     * @param func Update type.
     * @param prev Previous value.
     * @param expected Expected value.
     * @return If we should update value.
     */
    private static boolean shouldUpdate(final int func, final Object prev, final Object expected) {
        switch (func) {
            case UpdateAlways:
                return true;
            case UpdateIfAbsent:
                return prev == null;
            case UpdateIfPresent:
                return prev != null;
            default: { // UpdateIfEq
                assert expected != null;

                if (prev == null) {
                    return false;
                }

                return prev.equals(expected);
            }
        }
    }

    /**
     * @param func Update type.
     * @param prev Previous value.
     * @return Result object.
     */
    private static Object noUpdateResult(final int func, final Object prev) {
        return func == UpdateIfEq ? Boolean.FALSE : prev;
    }

    /**
     * @param func Update type.
     * @param prev Previous value.
     * @return Result object.
     */
    private static Object updateResult(final int func, final Object prev) {
        return func == UpdateIfEq ? Boolean.TRUE : prev;
    }

    /**
     * @param func Update type.
     * @param result Operation result.
     * @param newValue New value.
     * @return Size delta.
     */
    private static int sizeDelta(final int func, final Object result, final Object newValue) {
        switch (func) {
            case UpdateAlways:
                return (result != null ? -1 : 0) + (newValue != null ? 1 : 0);

            case UpdateIfAbsent:
                assert(newValue != null);
                return result != null ? 0 : 1;

            case UpdateIfPresent:
                return result == null ? 0 : (newValue != null ? 0 : -1);

            default:  // UpdateIfEq
                return !((Boolean) result) ? 0 : (newValue != null ? 0 : -1);
        }
    }

    /** {@inheritDoc} */
    @Override public V put(final K key, final V value) {
        return (V)update(key, UpdateAlways, null, value);
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(final K key, final V value) {
        return (V)update(key, UpdateIfAbsent, null, value);
    }

    /** {@inheritDoc} */
    @Override public V replace(final K key, final V value) {
        return (V)update(key, UpdateIfPresent, null, value);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(final K key, final V oldValue, final V newValue) {
        return (Boolean) update(key, UpdateIfEq, oldValue, newValue);
    }

    /** {@inheritDoc} */
    @Override public V remove(final Object key) {
        return (V)update((K)key, UpdateAlways, null, null);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(final Object key, final Object value) {
        if (key == null) {
            throw new NullPointerException();
        }

        if (value == null) {
            return false;
        }

        return (Boolean) update((K)key, UpdateIfEq, (V)value, null);
    }

    /**
     * @param key Key.
     * @param func Update type.
     * @param expected Expected value.
     * @param newValue New value.
     * @return Result object.
     */
    private Object update(final K key, final int func, final V expected, final V newValue) {
        final Comparable<? super K> k = comparable(key);

        LongArray unlinked = new LongArray();

        int sd = 0;

        try {
            final Object result = updateUnderRoot(key, k, func, expected, newValue, holderRef, unlinked);

            sd = sizeDelta(func, result, newValue);

            if (sd != 0)
                size.addAndGet(sd);

            return result;
        }
        finally {
            deallocate(unlinked);
        }
    }

    /**
     * Deallocates given node pointers.
     *
     * @param unlinked Array of unlinked pointers.
     */
    private void deallocate(LongArray unlinked) {
        RecycleQueue toMem = null;
        RecycleQueue toSnap = null;

        for (int i = 0; i < unlinked.idx; i++) {
            long ptr = unlinked.arr[i];

            if (ptr > 0) { // Unlinked.
                if (toMem == null)
                    toMem = new RecycleQueue();

                toMem.add(ptr);
            }
            else { // Deallocated by lazyCopy.
                assert ptr != 0;

                if (toSnap == null)
                    toSnap = new RecycleQueue();

                toSnap.add(-ptr);
            }
        }

        if (toSnap != null) {
            for (GridOffHeapSnapTreeMap snap : snapshots.values()) {
                if (snap.recycleBin.add(toSnap)) {
                    toSnap = null;

                    break;
                }
            }

            if (toSnap != null) {
                // Can't add to snapshot queues, will deallocate all together.
                if (toMem == null)
                    toMem = toSnap;
                else
                    toMem.add(toSnap);
            }
        }

        if (toMem != null)
            guard.releaseLater(toMem);
    }

    /**
     * @param q Recycle queue.
     */
    private void doDeallocateSnapshot(RecycleQueue q) {
        for (GridOffHeapSnapTreeMap s : snapshots.tailMap(snapshotId, false).values()) {
            // Try to add our garbage to older alive snapshot which can use the same nodes.
            if (s.recycleBin.add(q))
                return;
        }

        // No active updates.
        q.deallocate();
    }

    /**
     * Manages updates to the root holder.
     *
     * @param key Key.
     * @param k Comparator.
     * @param func Update type.
     * @param expected Expected value.
     * @param newValue New value.
     * @param holder Root holder.
     * @param unlinked Array for unlinked nodes.
     * @return Result object.
     */
    @SuppressWarnings("unchecked")
    private Object updateUnderRoot(final K key, final Comparable<? super K> k, final int func, final V expected,
        final V newValue, final long holder, LongArray unlinked) {
        int i = 0;

        while (true) {
            final long right = unsharedRight(holder, unlinked);

            if (right == 0) {
                // key is not present
                if (!shouldUpdate(func, null, expected)) {
                    return noUpdateResult(func, null);
                }

                if (newValue == null || attemptInsertIntoEmpty(key, newValue, holder)) {
                    // nothing needs to be done, or we were successful, prev value is Absent
                    return updateResult(func, null);
                }
                // else RETRY
            }
            else {
                final long ovl = shrinkOVL(right);

                if (isShrinkingOrUnlinked(ovl)) {
                    waitUntilShrinkCompleted(right, ovl);
                    // RETRY
                }
                else if (right == right(holder)) {
                    // this is the protected .right
                    final Object vo = attemptUpdate(key, k, func, expected, newValue, holder, right, ovl, unlinked);

                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    /**
     * @param key Key.
     * @param vOpt Value.
     * @param holder Root holder.
     * @return {@code true} If succeeded.
     */
    private boolean attemptInsertIntoEmpty(final K key, final V vOpt, final long holder) {
        KeyLock.Lock l = lock.lock(holder);

        try {
            if (right(holder) == 0) {
                right(holder, newNode(key, 1, vOpt, holder, 0L, 0L));

                height(holder, 2);

                return true;
            }
            else {
                return false;
            }
        }
        finally {
            if (l != null)
                l.unlock();
        }
    }

    /**
     * Checks if we operate by valid node version.
     *
     * @param node Node pointer.
     * @param nodeOVL Node version.
     * @return {@code True} if node version changed or node was unlinked.
     */
    private boolean isOutdatedOVL(long node, long nodeOVL) {
        return shrinkOVL(node) != nodeOVL;
    }

    /** If successful returns the non-null previous value, SpecialNull for a
     *  null previous value, or null if not previously in the map.
     *  The caller should retry if this method returns SpecialRetry.
     */
    @SuppressWarnings("unchecked")
    private Object attemptUpdate(final Object key, final Comparable<? super K> k, final int func, final V expected,
        final V newValue, final long parent, final long node, final long nodeOVL, LongArray unlinked) {
        // As the search progresses there is an implicit min and max assumed for the
        // branch of the tree rooted at node. A left rotation of a node x results in
        // the range of keys in the right branch of x being reduced, so if we are at a
        // node and we wish to traverse to one of the branches we must make sure that
        // the node has not undergone a rotation since arriving from the parent.
        //
        // A rotation of node can't screw us up once we have traversed to node's
        // child, so we don't need to build a huge transaction, just a chain of
        // smaller read-only transactions.

        assert !isUnlinked(nodeOVL);

        final int cmp = k.compareTo(key(node));

        if (cmp == 0) {
            return attemptNodeUpdate(func, expected, newValue, parent, node, unlinked);
        }

        final char dirToC = cmp < 0 ? Left : Right;

        int i = 0;

        while (true) {
            if (isOutdatedOVL(node, nodeOVL)) {
                return SpecialRetry;
            }

            final long child = unsharedChild(node, dirToC, unlinked);

            if (child == 0) {
                // key is not present
                if (newValue == null) {
                    // Removal is requested.  Read of node.child occurred
                    // while parent.child was valid, so we were not affected
                    // by any shrinks.
                    KeyLock.Lock l = lock.lock(node);

                    try {
                        if (isOutdatedOVL(node, nodeOVL)) {
                            return SpecialRetry;
                        }

                        return null;
                    }
                    finally {
                        if (l != null)
                            l.unlock();
                    }
                }
                else {
                    // Update will be an insert.
                    final boolean success;
                    final long damaged;

                    KeyLock.Lock l = lock.lock(node);

                    try {
                        // Validate that we haven't been affected by past
                        // rotations.  We've got the lock on node, so no future
                        // rotations can mess with us.
                        if (isOutdatedOVL(node, nodeOVL)) {
                            return SpecialRetry;
                        }

                        if (child(node, dirToC) != 0) {
                            // Lost a race with a concurrent insert.  No need
                            // to back up to the parent, but we must RETRY in
                            // the outer loop of this method.
                            success = false;
                            damaged = 0;
                        }
                        else {
                            // We're valid.  Does the user still want to
                            // perform the operation?
                            if (!shouldUpdate(func, null, expected)) {
                                return noUpdateResult(func, null);
                            }

                            // Create a new leaf
                            setChild(node, dirToC, newNode((K)key, 1, newValue,
                                node, 0L, 0L));
                            success = true;

                            // attempt to fix node.height while we've still got
                            // the lock
                            damaged = fixHeight_nl(node);
                        }
                    }
                    finally {
                        if (l != null)
                            l.unlock();
                    }

                    if (success) {
                        fixHeightAndRebalance(damaged, unlinked);

                        return updateResult(func, null);
                    }
                    // else RETRY
                }
            }
            else {
                // non-null child
                final long childOVL = shrinkOVL(child);

                if (isShrinkingOrUnlinked(childOVL)) {
                    waitUntilShrinkCompleted(child, childOVL);
                    // RETRY
                }
                else if (child != child(node, dirToC)) {
                    // this second read is important, because it is protected
                    // by childOVL
                    // RETRY
                }
                else {
                    // validate the read that our caller took to get to node
                    if (isOutdatedOVL(node, nodeOVL)) {
                        return SpecialRetry;
                    }

                    // At this point we know that the traversal our parent took
                    // to get to node is still valid.  The recursive
                    // implementation will validate the traversal from node to
                    // child, so just prior to the nodeOVL validation both
                    // traversals were definitely okay.  This means that we are
                    // no longer vulnerable to node shrinks, and we don't need
                    // to validate nodeOVL any more.
                    final Object vo = attemptUpdate(key, k, func, expected, newValue, node, child, childOVL, unlinked);

                    if (vo != SpecialRetry) {
                        return vo;
                    }
                    // else RETRY
                }
            }
        }
    }

    /**
     * Parent will only be used for unlink, update can proceed even if parent is stale.
     */
    private Object attemptNodeUpdate(final int func, final V expected, final V newValue, final long parent,
        final long node, LongArray unlinked) {
        if (newValue == null) {
            // removal
            if (vOptIsNull(node)) {
                // This node is already removed, nothing to do.
                return null;
            }
        }

        if (newValue == null && (left(node) == 0 || right(node) == 0)) {
            // potential unlink, get ready by locking the parent
            final Object prev;
            final long damaged;

            KeyLock.Lock pl = lock.lock(parent);

            try {
                if (isUnlinked(shrinkOVL(parent)) || parent(node) != parent) {
                    return SpecialRetry;
                }

                KeyLock.Lock nl = lock.lock(node);

                try {
                    if (isUnlinked(shrinkOVL(node))) {
                        return SpecialRetry;
                    }

                    assert parent(node) == parent && parent > 0;

                    prev = vOpt(node);

                    if (!shouldUpdate(func, prev, expected)) {
                        return noUpdateResult(func, prev);
                    }

                    if (prev == null) {
                        return updateResult(func, prev);
                    }

                    if (!attemptUnlink_nl(parent, node, unlinked)) {
                        return SpecialRetry;
                    }
                }
                finally {
                    if (nl != null)
                        nl.unlock();
                }

                // try to fix the parent while we've still got the lock
                damaged = fixHeight_nl(parent);
            }
            finally {
                if (pl != null)
                    pl.unlock();
            }

            fixHeightAndRebalance(damaged, unlinked);

            return updateResult(func, prev);
        }
        else {
            // potential update (including remove-without-unlink)
            KeyLock.Lock l = lock.lock(node);

            try {
                // regular version changes don't bother us
                if (isUnlinked(shrinkOVL(node))) {
                    return SpecialRetry;
                }

                final Object prev = vOpt(node);

                if (!shouldUpdate(func, prev, expected)) {
                    return noUpdateResult(func, prev);
                }

                // retry if we now detect that unlink is possible
                if (newValue == null && (left(node) == 0 || right(node) == 0)) {
                    return SpecialRetry;
                }

                // update in-place
                vOpt(node, newValue);

                afterNodeUpdate_nl(node, newValue);

                return updateResult(func, prev);
            }
            finally {
                if (l != null)
                    l.unlock();
            }
        }
    }

    /**
     * Special protected method to be overridden by extending classes.
     * @param node Node pointer.
     * @param val Value.
     */
    protected void afterNodeUpdate_nl(long node, V val) {
        // No-op.
    }

    /**
     * Does not adjust the size or any heights.
     */
    private boolean attemptUnlink_nl(final long parent, final long node, LongArray unlinked) {
        // assert (Thread.holdsLock(parent));
        // assert (Thread.holdsLock(node));
        assert (!isUnlinked(shrinkOVL(parent)));

        final long parentL = left(parent);
        final long  parentR = right(parent);

        if (parentL != node && parentR != node) {
            // node is no longer a child of parent
            return false;
        }

        long nodeOVL = shrinkOVL(node);

        assert (!isUnlinked(nodeOVL));
        assert (parent == parent(node));

        final long left = unsharedLeft(node, unlinked);
        final long right = unsharedRight(node, unlinked);

        if (left != 0 && right != 0) {
            // splicing is no longer possible
            return false;
        }

        final long splice = left != 0 ? left : right;

        if (parentL == node) {
            left(parent, splice);
        }
        else {
            right(parent, splice);
        }

        if (splice != 0) {
            parent(splice, parent);
        }

        shrinkOVL(node, UnlinkedOVL);
        vOpt(node, null);

        unlinked.add(node);

        return true;
    }

    /** {@inheritDoc} */
    @Override public Map.Entry<K,V> pollFirstEntry() {
        LongArray unlinked = new LongArray();

        try {
            return pollExtremeEntry(Left, unlinked);
        }
        finally {
            deallocate(unlinked);
        }
    }

    /** {@inheritDoc} */
    @Override public Map.Entry<K,V> pollLastEntry() {
        LongArray unlinked = new LongArray();

        try {
            return pollExtremeEntry(Right, unlinked);
        }
        finally {
            deallocate(unlinked);
        }
    }

    /**
     * @param dir Direction.
     * @param unlinked Array for unlinked.
     * @return Entry.
     */
    private Map.Entry<K,V> pollExtremeEntry(final char dir, LongArray unlinked) {
        int sizeDelta = 0;

        final Map.Entry<K,V> prev = pollExtremeEntryUnderRoot(dir, holderRef, unlinked);

        if (prev != null) {
            sizeDelta = -1;
        }

        return prev;
    }

    /**
     * @param dir Direction.
     * @param holder Root holder.
     * @param unlinked Array for unlinked nodes.
     * @return Entry.
     */
    private Map.Entry<K,V> pollExtremeEntryUnderRoot(final char dir, final long holder, LongArray unlinked) {
        while (true) {
            final long right = unsharedRight(holder, unlinked);

            if (right == 0) {
                // tree is empty, nothing to remove
                return null;
            }
            else {
                final long ovl = shrinkOVL(right);

                if (isShrinkingOrUnlinked(ovl)) {
                    waitUntilShrinkCompleted(right, ovl);
                    // RETRY
                }
                else if (right == right(holder)) {
                    // this is the protected .right
                    final Map.Entry<K,V> result = attemptRemoveExtreme(dir, holder, right, ovl, unlinked);

                    if (result != SpecialRetry) {
                        return result;
                    }
                    // else RETRY
                }
            }
        }
    }

    /**
     * @param dir Direction.
     * @param parent Parent.
     * @param node Node.
     * @param nodeOVL OVL.
     * @param unlinked Array for unlinked nodes.
     * @return Entry.
     */
    private Map.Entry<K,V> attemptRemoveExtreme(final char dir, final long parent, final long node, final long nodeOVL,
        LongArray unlinked) {
        assert !isUnlinked(nodeOVL);

        while (true) {
            final long child = unsharedChild(node, dir, unlinked);

            if (isOutdatedOVL(node, nodeOVL)) {
                return null;
            }

            if (child == 0) {
                // potential unlink, get ready by locking the parent
                final Object vo;
                final long damaged;

                KeyLock.Lock pl = lock.lock(parent);

                try {
                    if (isUnlinked(shrinkOVL(parent)) || parent(node) != parent) {
                        return null;
                    }

                    KeyLock.Lock nl = lock.lock(node);

                    try {
                        vo = vOpt(node);

                        if (child(node, dir) != 0 || !attemptUnlink_nl(parent, node, unlinked)) {
                            return null;
                        }
                        // success!
                    }
                    finally {
                        if (nl != null)
                            nl.unlock();
                    }

                    // try to fix parent.height while we've still got the lock
                    damaged = fixHeight_nl(parent);
                }
                finally {
                    if (pl != null)
                        pl.unlock();
                }

                fixHeightAndRebalance(damaged, unlinked);

                return new SimpleImmutableEntry<K,V>(key(node), (V)vo);
            }
            else {
                // keep going down
                final long childOVL = shrinkOVL(child);

                if (isShrinkingOrUnlinked(childOVL)) {
                    waitUntilShrinkCompleted(child, childOVL);
                    // RETRY
                }
                else if (child != child(node, dir)) {
                    // this second read is important, because it is protected
                    // by childOVL
                    // RETRY
                }
                else {
                    // validate the read that our caller took to get to node
                    if (isOutdatedOVL(node, nodeOVL)) {
                        return null;
                    }

                    final Map.Entry<K,V> result = attemptRemoveExtreme(dir, node, child, childOVL, unlinked);

                    if (result != null) {
                        return result;
                    }
                    // else RETRY
                }
            }
        }
    }

    /** */
    private static final int UnlinkRequired = -1;

    /** */
    private static final int RebalanceRequired = -2;

    /** */
    private static final int NothingRequired = -3;

    /**
     * @param node Node.
     * @return Condition.
     */
    private int nodeCondition(final long node) {
        // Begin atomic.
        final long nL = left(node);
        final long nR = right(node);

        if ((nL == 0 || nR == 0) && vOptIsNull(node)) {
            return UnlinkRequired;
        }

        final int hN = height(node);
        final int hL0 = height(nL);
        final int hR0 = height(nR);

        // End atomic.  Since any thread that changes a node promises to fix
        // it, either our read was consistent (and a NothingRequired conclusion
        // is correct) or someone else has taken responsibility for either node
        // or one of its children.

        final int hNRepl = 1 + Math.max(hL0, hR0);
        final int bal = hL0 - hR0;

        if (bal < -1 || bal > 1) {
            return RebalanceRequired;
        }

        return hN != hNRepl ? hNRepl : NothingRequired;
    }

    /**
     * @param node Node.
     * @param unlinked Array for unlinked nodes.
     */
    private void fixHeightAndRebalance(long node, LongArray unlinked) {
        while (node != 0 && parent(node) > 0) {
            final int condition = nodeCondition(node);

            if (condition == NothingRequired || isUnlinked(shrinkOVL(node))) {
                // nothing to do, or no point in fixing this node
                return;
            }

            if (condition != UnlinkRequired && condition != RebalanceRequired) {
                long n = node;

                KeyLock.Lock l = lock.lock(n);

                try {
                    if (isUnlinked(shrinkOVL(node)))
                        return;

                    node = fixHeight_nl(node);
                }
                finally {
                    if (l != null)
                        l.unlock();
                }
            }
            else {
                final long nParent = parent(node);

                if (nParent <= 0)
                    continue;

                KeyLock.Lock pl = lock.lock(nParent);

                try {
                    if (!isUnlinked(shrinkOVL(nParent)) && parent(node) == nParent) {
                        long n = node;

                        KeyLock.Lock nl = lock.lock(n);

                        try {
                            if (isUnlinked(shrinkOVL(node)))
                                return;

                            node = rebalance_nl(nParent, node, unlinked);
                        }
                        finally {
                            if (nl != null)
                                nl.unlock();
                        }
                    }
                    // else RETRY
                }
                finally {
                    if (pl != null)
                        pl.unlock();
                }
            }
        }
    }

    /**
     *  Attempts to fix the height of a (locked) damaged node, returning the
     *  lowest damaged node for which this thread is responsible.  Returns null
     *  if no more repairs are needed.
     */
    private long fixHeight_nl(final long node) {
        final int c = nodeCondition(node);
        switch (c) {
            case RebalanceRequired:
            case UnlinkRequired:
                // can't repair
                return node;

            case NothingRequired:
                // Any future damage to this node is not our responsibility.
                return 0;

            default:
                height(node, c);

                // we've damaged our parent, but we can't fix it now
                return parent(node);
        }
    }

    /**
     *  nParent and n must be locked on entry.  Returns a damaged node, or null
     *  if no more rebalancing is necessary.
     */
    private long rebalance_nl(final long nParent, final long n, LongArray unlinked) {
        final long nL = unsharedLeft(n, unlinked);
        final long nR = unsharedRight(n, unlinked);

        if ((nL == 0 || nR == 0) && vOptIsNull(n)) {
            if (attemptUnlink_nl(nParent, n, unlinked)) {
                // attempt to fix nParent.height while we've still got the lock
                return fixHeight_nl(nParent);
            }
            else {
                // retry needed for n
                return n;
            }
        }

        final int hN = height(n);
        final int hL0 = height(nL);
        final int hR0 = height(nR);
        final int hNRepl = 1 + Math.max(hL0, hR0);
        final int bal = hL0 - hR0;

        if (bal > 1) {
            return rebalanceToRight_nl(nParent, n, nL, hR0, unlinked);
        }
        else if (bal < -1) {
            return rebalanceToLeft_nl(nParent, n, nR, hL0, unlinked);
        }
        else if (hNRepl != hN) {
            // we've got more than enough locks to do a height change, no need to
            // trigger a retry
            height(n, hNRepl);

            // nParent is already locked, let's try to fix it too
            return fixHeight_nl(nParent);
        }
        else {
            // nothing to do
            return 0;
        }
    }

    /**
     * @param nParent Parent.
     * @param n Node.
     * @param nL Left child.
     * @param hR0 Height of right child.
     * @param unlinked Array for unlinked nodes.
     * @return Node.
     */
    private long rebalanceToRight_nl(final long nParent, final long n, final long nL, final int hR0,
        LongArray unlinked) {
        // L is too large, we will rotate-right.  If L.R is taller
        // than L.L, then we will first rotate-left L.
        KeyLock.Lock nLl = lock.lock(nL);

        try {
            final int hL = height(nL);

            if (hL - hR0 <= 1) {
                return n; // retry
            }
            else {
                final long nLR = unsharedRight(nL, unlinked);
                final int hLL0 = height(left(nL));
                final int hLR0 = height(nLR);

                if (hLL0 >= hLR0) {
                    // rotate right based on our snapshot of hLR
                    return rotateRight_nl(nParent, n, nL, hR0, hLL0, nLR, hLR0);
                }
                else {
                    KeyLock.Lock nLRl = lock.lock(nLR);

                    try {
                        // If our hLR snapshot is incorrect then we might
                        // actually need to do a single rotate-right on n.
                        final int hLR = height(nLR);

                        if (hLL0 >= hLR) {
                            return rotateRight_nl(nParent, n, nL, hR0, hLL0, nLR, hLR);
                        }
                        else {
                            // If the underlying left balance would not be
                            // sufficient to actually fix n.left, then instead
                            // of rolling it into a double rotation we do it on
                            // it's own.  This may let us avoid rotating n at
                            // all, but more importantly it avoids the creation
                            // of damaged nodes that don't have a direct
                            // ancestry relationship.  The recursive call to
                            // rebalanceToRight_nl in this case occurs after we
                            // release the lock on nLR.
                            //
                            // We also need to avoid damaging n.left if post-
                            // rotation it would be an unnecessary routing node.
                            // Note that although our height snapshots might be
                            // stale, their zero/non-zero state can't be.
                            final int hLRL = height(left(nLR));
                            final int b = hLL0 - hLRL;

                            if (b >= -1 && b <= 1 && !((hLL0 == 0 || hLRL == 0) && vOptIsNull(nL))) {
                                // nParent.child.left won't be damaged after a double rotation
                                return rotateRightOverLeft_nl(nParent, n, nL, hR0, hLL0, nLR, hLRL, unlinked);
                            }
                        }
                    }
                    finally {
                        if (nLRl != null)
                            nLRl.unlock();
                    }

                    // focus on nL, if necessary n will be balanced later
                    return rebalanceToLeft_nl(n, nL, nLR, hLL0, unlinked);
                }
            }
        }
        finally {
            if (nLl != null)
                nLl.unlock();
        }
    }

    /**
     * @param nParent Parent.
     * @param n Node.
     * @param nR Right child.
     * @param hL0 Head of left child.
     * @param unlinked Array for unlinked nodes.
     * @return Node.
     */
    private long rebalanceToLeft_nl(final long nParent, final long n, final long nR, final int hL0,
        LongArray unlinked) {
        KeyLock.Lock nRl = lock.lock(nR);

        try {
            final int hR = height(nR);

            if (hL0 - hR >= -1) {
                return n; // retry
            }
            else {
                final long nRL = unsharedLeft(nR, unlinked);
                final int hRL0 = height(nRL);
                final int hRR0 = height(right(nR));

                if (hRR0 >= hRL0) {
                    return rotateLeft_nl(nParent, n, hL0, nR, nRL, hRL0, hRR0);
                }
                else {
                    KeyLock.Lock nRLl = lock.lock(nRL);

                    try {
                        final int hRL = height(nRL);

                        if (hRR0 >= hRL) {
                            return rotateLeft_nl(nParent, n, hL0, nR, nRL, hRL, hRR0);
                        }
                        else {
                            final int hRLR = height(right(nRL));
                            final int b = hRR0 - hRLR;

                            if (b >= -1 && b <= 1 && !((hRR0 == 0 || hRLR == 0) && vOptIsNull(nR))) {
                                return rotateLeftOverRight_nl(nParent, n, hL0, nR, nRL, hRR0, hRLR, unlinked);
                            }
                        }
                    }
                    finally {
                        if (nRLl != null)
                            nRLl.unlock();
                    }

                    return rebalanceToRight_nl(n, nR, nRL, hRR0, unlinked);
                }
            }
        }
        finally {
            if (nRl != null)
                nRl.unlock();
        }
    }

    /**
     * @param nParent Parent.
     * @param n Node.
     * @param nL Left child.
     * @param hR Height of right.
     * @param hLL Height of left-left child.
     * @param nLR Left-right child.
     * @param hLR Height of left-right child.
     * @return Node.
     */
    private long rotateRight_nl(final long nParent, final long n, final long nL, final int hR, final int hLL,
        final long nLR, final int hLR) {
        final long nodeOVL = shrinkOVL(n);

        assert !isShrinkingOrUnlinked(nodeOVL);

        final long nPL = left(nParent);

        shrinkOVL(n, beginChange(nodeOVL));

        left(n, nLR);

        if (nLR != 0) {
            parent(nLR, n);
        }

        right(nL, n);
        parent(n, nL);

        if (nPL == n) {
            left(nParent, nL);
        }
        else {
            right(nParent, nL);
        }

        parent(nL, nParent);

        // fix up heights links
        final int hNRepl = 1 + Math.max(hLR, hR);
        height(n, hNRepl);
        height(nL, 1 + Math.max(hLL, hNRepl));

        shrinkOVL(n, endChange(nodeOVL));

        // We have damaged nParent, n (now parent.child.right), and nL (now
        // parent.child).  n is the deepest.  Perform as many fixes as we can
        // with the locks we've got.

        // We've already fixed the height for n, but it might still be outside
        // our allowable balance range.  In that case a simple fixHeight_nl
        // won't help.
        final int balN = hLR - hR;

        if (balN < -1 || balN > 1) {
            // we need another rotation at n
            return n;
        }

        // we've fixed balance and height damage for n, now handle
        // extra-routing node damage
        if ((nLR == 0 || hR == 0) && vOptIsNull(n)) {
            // we need to remove n and then repair
            return n;
        }

        // we've already fixed the height at nL, do we need a rotation here?
        final int balL = hLL - hNRepl;

        if (balL < -1 || balL > 1) {
            return nL;
        }

        // nL might also have routing node damage (if nL.left was null)
        if (hLL == 0 && vOptIsNull(nL)) {
            return nL;
        }

        // try to fix the parent height while we've still got the lock
        return fixHeight_nl(nParent);
    }

    /**
     * @param nParent Parent.
     * @param n Node.
     * @param hL Height of left child.
     * @param nR Right child.
     * @param nRL Right-left child.
     * @param hRL Height of right-left child.
     * @param hRR Height of right-right child.
     * @return Node.
     */
    private long rotateLeft_nl(final long nParent, final long n, final int hL, final long nR, final long nRL,
        final int hRL, final int hRR) {
        final long nodeOVL = shrinkOVL(n);

        assert !isShrinkingOrUnlinked(nodeOVL);

        final long nPL = left(nParent);

        shrinkOVL(n, beginChange(nodeOVL));

        // fix up n links, careful to be compatible with concurrent traversal for all but n
        right(n, nRL);

        if (nRL != 0) {
            parent(nRL, n);
        }

        left(nR, n);
        parent(n, nR);

        if (nPL == n) {
            left(nParent, nR);
        }
        else {
            right(nParent, nR);
        }

        parent(nR, nParent);

        // fix up heights
        final int  hNRepl = 1 + Math.max(hL, hRL);

        height(n, hNRepl);
        height(nR, 1 + Math.max(hNRepl, hRR));

        shrinkOVL(n, endChange(nodeOVL));

        final int balN = hRL - hL;

        if (balN < -1 || balN > 1) {
            return n;
        }

        if ((nRL == 0 || hL == 0) && vOptIsNull(n)) {
            return n;
        }

        final int balR = hRR - hNRepl;

        if (balR < -1 || balR > 1) {
            return nR;
        }

        if (hRR == 0 && vOptIsNull(nR)) {
            return nR;
        }

        return fixHeight_nl(nParent);
    }

    /**
     * @param nParent Parent.
     * @param n Node.
     * @param nL Left child.
     * @param hR Height of right child.
     * @param hLL Height of left-left child.
     * @param nLR Left-right child.
     * @param hLRL Height of left-right-left child.
     * @param unlinked Array for unlinked nodes.
     * @return Node.
     */
    private long rotateRightOverLeft_nl(final long nParent, final long n, final long nL, final int hR, final int hLL,
        final long nLR, final int hLRL, LongArray unlinked) {
        final long nodeOVL = shrinkOVL(n);
        final long leftOVL = shrinkOVL(nL);

        final long nPL = left(nParent);
        final long nLRL = unsharedLeft(nLR, unlinked);
        final long nLRR = unsharedRight(nLR, unlinked);
        final int hLRR = height(nLRR);

        assert !isShrinkingOrUnlinked(nodeOVL);
        assert !isShrinkingOrUnlinked(leftOVL);

        shrinkOVL(n, beginChange(nodeOVL));
        shrinkOVL(nL, beginChange(leftOVL));

        // fix up n links, careful about the order!
        left(n, nLRR);

        if (nLRR != 0) {
            parent(nLRR, n);
        }

        right(nL, nLRL);

        if (nLRL != 0) {
            parent(nLRL, nL);
        }

        left(nLR, nL);
        parent(nL, nLR);

        right(nLR, n);
        parent(n, nLR);

        if (nPL == n) {
            left(nParent, nLR);
        }
        else {
            right(nParent, nLR);
        }

        parent(nLR, nParent);

        // fix up heights
        final int hNRepl = 1 + Math.max(hLRR, hR);
        height(n, hNRepl);

        final int hLRepl = 1 + Math.max(hLL, hLRL);
        height(nL, hLRepl);
        height(nLR, 1 + Math.max(hLRepl, hNRepl));

        shrinkOVL(n, endChange(nodeOVL));
        shrinkOVL(nL, endChange(leftOVL));

        // caller should have performed only a single rotation if nL was going
        // to end up damaged
        assert(Math.abs(hLL - hLRL) <= 1);
        assert(!((hLL == 0 || nLRL == 0) && vOptIsNull(nL)));

        // We have damaged nParent, nLR (now parent.child), and n (now
        // parent.child.right).  n is the deepest.  Perform as many fixes as we
        // can with the locks we've got.

        // We've already fixed the height for n, but it might still be outside
        // our allowable balance range.  In that case a simple fixHeight_nl
        // won't help.
        final int balN = hLRR - hR;

        if (balN < -1 || balN > 1) {
            // we need another rotation at n
            return n;
        }

        // n might also be damaged by being an unnecessary routing node
        if ((nLRR == 0 || hR == 0) && vOptIsNull(n)) {
            // repair involves splicing out n and maybe more rotations
            return n;
        }

        // we've already fixed the height at nLR, do we need a rotation here?
        final int balLR = hLRepl - hNRepl;

        if (balLR < -1 || balLR > 1) {
            return nLR;
        }

        // try to fix the parent height while we've still got the lock
        return fixHeight_nl(nParent);
    }

    /**
     * @param nParent Parent.
     * @param n Node.
     * @param hL Height of left child.
     * @param nR Right child.
     * @param nRL Right-left child.
     * @param hRR Height of right-right child.
     * @param hRLR Height of right-left-right child.
     * @param unlinked Array for unlinked nodes.
     * @return Node.
     */
    private long rotateLeftOverRight_nl(final long nParent, final long n, final int hL, final long nR, final long nRL,
        final int hRR, final int hRLR, LongArray unlinked) {
        final long nodeOVL = shrinkOVL(n);
        final long rightOVL = shrinkOVL(nR);

        final long nPL = left(nParent);
        final long nRLL = unsharedLeft(nRL, unlinked);

        final long nRLR = unsharedRight(nRL, unlinked);
        final int hRLL = height(nRLL);

        assert !isShrinkingOrUnlinked(nodeOVL);
        assert !isShrinkingOrUnlinked(rightOVL);

        shrinkOVL(n, beginChange(nodeOVL));
        shrinkOVL(nR, beginChange(rightOVL));

        // fix up n links, careful about the order!
        right(n, nRLL);

        if (nRLL != 0) {
            parent(nRLL, n);
        }

        left(nR, nRLR);

        if (nRLR != 0) {
            parent(nRLR, nR);
        }

        right(nRL, nR);
        parent(nR, nRL);

        left(nRL, n);
        parent(n, nRL);

        if (nPL == n) {
            left(nParent, nRL);
        }
        else {
            right(nParent, nRL);
        }

        parent(nRL, nParent);

        // fix up heights
        final int hNRepl = 1 + Math.max(hL, hRLL);
        height(n, hNRepl);

        final int hRRepl = 1 + Math.max(hRLR, hRR);
        height(nR, hRRepl);
        height(nRL, 1 + Math.max(hNRepl, hRRepl));

        shrinkOVL(n, endChange(nodeOVL));
        shrinkOVL(nR, endChange(rightOVL));

        assert(Math.abs(hRR - hRLR) <= 1);

        final int balN = hRLL - hL;

        if (balN < -1 || balN > 1) {
            return n;
        }

        if ((nRLL == 0 || hL == 0) && vOptIsNull(n)) {
            return n;
        }

        final int balRL = hRRepl - hNRepl;

        if (balRL < -1 || balRL > 1) {
            return nRL;
        }

        return fixHeight_nl(nParent);
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<K> keySet() {
        return navigableKeySet();
    }

    /** {@inheritDoc} */
    @Override public Set<Map.Entry<K,V>> entrySet() {
        return new EntrySet();
    }

    /**
     * Entry set.
     */
    private class EntrySet extends AbstractSet<Map.Entry<K,V>> {
        /** {@inheritDoc} */
        @Override public int size() {
            return GridOffHeapSnapTreeMap.this.size();
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return GridOffHeapSnapTreeMap.this.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            GridOffHeapSnapTreeMap.this.clear();
        }

        /** {@inheritDoc} */
        @Override public boolean contains(final Object o) {
            if (!(o instanceof Map.Entry<?,?>)) {
                return false;
            }

            final Object k = ((Map.Entry<?,?>)o).getKey();
            final Object v = ((Map.Entry<?,?>)o).getValue();

            final GridOffHeapSmartPointer actual = GridOffHeapSnapTreeMap.this.getImpl((K)k);

            if (actual == null) {
                // no associated value
                return false;
            }

            return v.equals(actual);
        }

        /** {@inheritDoc} */
        @Override public boolean add(final Entry<K,V> e) {
            final V v = e.getValue();

            if (v == null)
                throw new NullPointerException();

            return !v.equals(update(e.getKey(), UpdateAlways, null, v));
        }

        /** {@inheritDoc} */
        @Override public boolean remove(final Object o) {
            if (!(o instanceof Map.Entry<?,?>)) {
                return false;
            }

            final Object k = ((Map.Entry<?,?>)o).getKey();
            final Object v = ((Map.Entry<?,?>)o).getValue();

            return GridOffHeapSnapTreeMap.this.remove(k, v);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Entry<K,V>> iterator() {
            return new EntryIter(GridOffHeapSnapTreeMap.this);
        }
    }

    /**
     * Entry iterator.
     */
    private class EntryIter extends AbstractIter implements Iterator<Entry<K,V>> {
        /**
         * @param m Map.
         */
        private EntryIter(final GridOffHeapSnapTreeMap m) {
            super(m);
        }

        /**
         * @param m Map.
         * @param minCmp Lower bound.
         * @param minIncl Include lower bound.
         * @param maxCmp Upper bound.
         * @param maxIncl Include upper bound.
         * @param descending Iterator in descending order.
         */
        private EntryIter(final GridOffHeapSnapTreeMap m, final Comparable<? super K> minCmp, final boolean minIncl,
            final Comparable<? super K> maxCmp, final boolean maxIncl, final boolean descending) {
            super(m, minCmp, minIncl, maxCmp, maxIncl, descending);
        }

        /** {@inheritDoc} */
        @Override public Entry next() {
            return new Entree(nextNode());
        }
    }

    /**
     * Key iterator.
     */
    private class KeyIter extends AbstractIter implements Iterator<GridOffHeapSmartPointer> {
        /**
         * @param m Map.
         */
        private KeyIter(final GridOffHeapSnapTreeMap m) {
            super(m);
        }

        /**
         * @param m Map.
         * @param minCmp Lower bound.
         * @param minIncl Include lower bound.
         * @param maxCmp Upper bound.
         * @param maxIncl Include upper bound.
         * @param descending Iterator in descending order.
         */
        private KeyIter(final GridOffHeapSnapTreeMap m, final Comparable<? super K> minCmp, final boolean minIncl,
            final Comparable<? super K> maxCmp, final boolean maxIncl, final boolean descending) {
            super(m, minCmp, minIncl, maxCmp, maxIncl, descending);
        }

        /** {@inheritDoc} */
        @Override public GridOffHeapSmartPointer next() {
            return key(nextNode());
        }
    }

    /**
     * Abstract iterator.
     */
    private class AbstractIter {
        /** */
        private final GridOffHeapSnapTreeMap m;

        /** */
        private final boolean descending;

        /** */
        private final char forward;

        /** */
        private final char reverse;

        /** */
        private long[] path;

        /** */
        private int depth = 0;

        /** */
        private long mostRecentNode;

        /** */
        private final GridOffHeapSmartPointer endKey;

        /** */
        private final long rootHolderSnapshot;

        /**
         * @param m Map.
         */
        @SuppressWarnings("unchecked")
        AbstractIter(final GridOffHeapSnapTreeMap m) {
            this.m = m;
            this.descending = false;
            this.forward = Right;
            this.reverse = Left;
            rootHolderSnapshot = m.holderRef;
            final long root = right(rootHolderSnapshot);
            this.path = new long[1 + height(root)];
            this.endKey = null;
            pushFirst(root);
        }

        /**
         * @param m Map.
         * @param minCmp Lower bound.
         * @param minIncl Include lower bound.
         * @param maxCmp Upper bound.
         * @param maxIncl Include upper bound.
         * @param descending Iterator in descending order.
         */
        @SuppressWarnings("unchecked")
        AbstractIter(final GridOffHeapSnapTreeMap m, final Comparable<? super K> minCmp, final boolean minIncl,
            final Comparable<? super K> maxCmp, final boolean maxIncl, final boolean descending) {
            this.m = m;
            this.descending = descending;
            this.forward = !descending ? Right : Left;
            this.reverse = !descending ? Left : Right;
            final Comparable<? super K> fromCmp;
            final boolean fromIncl = !descending ? minIncl : maxIncl;
            final Comparable<? super K> toCmp;
            final boolean toIncl = !descending ? maxIncl : minIncl;

            if (!descending) {
                fromCmp = minCmp;
                toCmp = maxCmp;
            }
            else {
                fromCmp = maxCmp;
                toCmp = minCmp;
            }

            rootHolderSnapshot = m.holderRef;
            final long root = right(rootHolderSnapshot);

            if (toCmp != null) {
                this.endKey = (GridOffHeapSmartPointer) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, true, forward);

                if (this.endKey == null) {
                    // no node satisfies the bound, nothing to iterate
                    // ---------> EARLY EXIT
                    return;
                }
            }
            else {
                this.endKey = null;
            }

            this.path = new long[1 + height(root)];

            if (fromCmp == null) {
                pushFirst(root);
            }
            else {
                pushFirst(root, fromCmp, fromIncl);

                if (depth > 0 && vOptIsNull(top())) {
                    advance();
                }
            }
        }

        /**
         * @param comparable Comparable.
         * @param key Key.
         * @return Result of comparison.
         */
        private int cmp(final Comparable<? super K> comparable, final K key) {
            final int c = comparable.compareTo(key);

            if (!descending) {
                return c;
            }
            else {
                return c == Integer.MIN_VALUE ? 1 : -c;
            }
        }

        /**
         * @param node Node.
         */
        private void pushFirst(long node) {
            while (node != 0) {
                path(node);

                node = child(node, reverse);
            }
        }

        /**
         * @param node Node.
         */
        private void path(long node) {
            if (depth == path.length)
                path = Arrays.copyOf(path, depth + 2);

            path[depth++] = node;
        }

        /**
         * @param node Node.
         * @param fromCmp Lower bound.
         * @param fromIncl Include lower bound.
         */
        private void pushFirst(long node, final Comparable<? super K> fromCmp, final boolean fromIncl) {
            while (node != 0) {
                final int c = cmp(fromCmp, key(node));

                if (c > 0 || (c == 0 && !fromIncl)) {
                    // everything we're interested in is on the right
                    node = child(node, forward);
                }
                else {
                    path(node);

                    if (c == 0) {
                        // start the iteration here
                        return;
                    }
                    else {
                        node = child(node, reverse);
                    }
                }
            }
        }

        /**
         * @return Top node.
         */
        private long top() {
            return path[depth - 1];
        }

        /**
         * Advance next node.
         */
        private void advance() {
            do {
                final long t = top();

                if (endKey != null && endKey.equals(key(t))) {
                    depth = 0;
                    path = null;

                    return;
                }

                final long fwd = child(t, forward);

                if (fwd != 0) {
                    pushFirst(fwd);
                }
                else {
                    // keep going up until we pop a node that is a left child
                    long popped;

                    do {
                        popped = path[--depth];
                    }
                    while (depth > 0 && popped == child(top(), forward));
                }

                if (depth == 0) {
                    // clear out the path so we don't pin too much stuff
                    path = null;

                    return;
                }

                // skip removed-but-not-unlinked entries
            }
            while (vOptIsNull(top()));
        }

        /**
         * @return {@code true} If iterator has next element.
         */
        public boolean hasNext() {
            return depth > 0;
        }

        /**
         * @return Next node.
         */
        long nextNode() {
            if (depth == 0) {
                throw new NoSuchElementException();
            }

            mostRecentNode = top();

            advance();

            return mostRecentNode;
        }

        /**
         * Remove current node.
         */
        public void remove() {
            if (mostRecentNode == 0) {
                throw new IllegalStateException();
            }

            m.remove(key(mostRecentNode));

            mostRecentNode = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<K> navigableKeySet() {
        return new KeySet(this) {
            @Override public Iterator<GridOffHeapSmartPointer> iterator() {
                return new KeyIter(GridOffHeapSnapTreeMap.this);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public NavigableSet<K> descendingKeySet() {
        return descendingMap().navigableKeySet();
    }

    /**
     * Key set.
     */
    private abstract static class KeySet<K> extends AbstractSet<K> implements NavigableSet<K> {
        /** */
        private final ConcurrentNavigableMap<K,?> map;

        /**
         * @param map Map.
         */
        protected KeySet(final ConcurrentNavigableMap<K,?> map) {
            this.map = map;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(final Object o) {
            return map.containsKey(o);
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return map.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return map.size();
        }

        /** {@inheritDoc} */
        @Override public boolean remove(final Object o) {
            return map.remove(o) != null;
        }

        /** {@inheritDoc} */
        @Override public Comparator<? super K> comparator() {
            return map.comparator();
        }

        /** {@inheritDoc} */
        @Override public K first() {
            return map.firstKey();
        }

        /** {@inheritDoc} */
        @Override public K last() {
            return map.lastKey();
        }

        /** {@inheritDoc} */
        @Override public K lower(final K k) {
            return map.lowerKey(k);
        }

        /** {@inheritDoc} */
        @Override public K floor(final K k) {
            return map.floorKey(k);
        }

        /** {@inheritDoc} */
        @Override public K ceiling(final K k) {
            return map.ceilingKey(k);
        }

        /** {@inheritDoc} */
        @Override public K higher(final K k) {
            return map.higherKey(k);
        }

        /** {@inheritDoc} */
        @Override public K pollFirst() {
            return map.pollFirstEntry().getKey();
        }

        /** {@inheritDoc} */
        @Override public K pollLast() {
            return map.pollLastEntry().getKey();
        }

        /** {@inheritDoc} */
        @Override public NavigableSet<K> descendingSet() {
            return map.descendingKeySet();
        }

        /** {@inheritDoc} */
        @Override public Iterator<K> descendingIterator() {
            return map.descendingKeySet().iterator();
        }

        /** {@inheritDoc} */
        @Override public NavigableSet<K> subSet(final K fromElement, final boolean minInclusive, final K toElement,
            final boolean maxInclusive) {
            return map.subMap(fromElement, minInclusive, toElement, maxInclusive).keySet();
        }

        /** {@inheritDoc} */
        @Override public NavigableSet<K> headSet(final K toElement, final boolean inclusive) {
            return map.headMap(toElement, inclusive).keySet();
        }

        /** {@inheritDoc} */
        @Override public NavigableSet<K> tailSet(final K fromElement, final boolean inclusive) {
            return map.tailMap(fromElement, inclusive).keySet();
        }

        /** {@inheritDoc} */
        @Override public SortedSet<K> subSet(final K fromElement, final K toElement) {
            return map.subMap(fromElement, toElement).keySet();
        }

        /** {@inheritDoc} */
        @Override public SortedSet<K> headSet(final K toElement) {
            return map.headMap(toElement).keySet();
        }

        /** {@inheritDoc} */
        @Override public SortedSet<K> tailSet(final K fromElement) {
            return map.tailMap(fromElement).keySet();
        }
    }

    /** {@inheritDoc} */
    @Override public ConcurrentNavigableMap<K,V> subMap(final K fromKey, final boolean fromInclusive, final K toKey,
        final boolean toInclusive) {
        final Comparable<? super K> fromCmp = comparable(fromKey);
        if (fromCmp.compareTo(toKey) > 0) {
            throw new IllegalArgumentException();
        }

        return new SubMap(this, fromKey, fromCmp, fromInclusive, toKey, comparable(toKey), toInclusive, false);
    }

    /** {@inheritDoc} */
    @Override public ConcurrentNavigableMap<K,V> headMap(final K toKey, final boolean inclusive) {
        return new SubMap(this, null, null, false, toKey, comparable(toKey), inclusive, false);
    }

    /** {@inheritDoc} */
    @Override public ConcurrentNavigableMap<K,V> tailMap(final K fromKey, final boolean inclusive) {
        return new SubMap(this, fromKey, comparable(fromKey), inclusive, null, null, false, false);
    }

    /** {@inheritDoc} */
    @Override public ConcurrentNavigableMap<K,V> subMap(final K fromKey, final K toKey) {
        return subMap(fromKey, true, toKey, false);
    }

    /** {@inheritDoc} */
    @Override public ConcurrentNavigableMap<K,V> headMap(final K toKey) {
        return headMap(toKey, false);
    }

    /** {@inheritDoc} */
    @Override public ConcurrentNavigableMap<K,V> tailMap(final K fromKey) {
        return tailMap(fromKey, true);
    }

    /** {@inheritDoc} */
    @Override public ConcurrentNavigableMap<K,V> descendingMap() {
        return new SubMap(this, null, null, false, null, null, false, true);
    }

    /**
     * Submap.
     */
    private class SubMap extends AbstractMap<K,V> implements ConcurrentNavigableMap<K,V> {
        /** */
        private final GridOffHeapSnapTreeMap<K,V> m;

        /** */
        private final GridOffHeapSmartPointer minKey;

        /** */
        private transient Comparable<? super K> minCmp;

        /** */
        private final boolean minIncl;

        /** */
        private final GridOffHeapSmartPointer maxKey;

        /** */
        private transient Comparable<? super K> maxCmp;

        /** */
        private final boolean maxIncl;

        /** */
        private final boolean descending;

        /**
         * @param m Map.
         * @param minKey Lower bound key.
         * @param minCmp Lower bound comparable.
         * @param minIncl Include lower bound.
         * @param maxKey Upper bound key.
         * @param maxCmp Upper bound comparable.
         * @param maxIncl Include upper bound.
         * @param descending Iterator in descending order.
         */
        private SubMap(final GridOffHeapSnapTreeMap<K,V> m, final GridOffHeapSmartPointer minKey,
            final Comparable<? super K> minCmp, final boolean minIncl, final GridOffHeapSmartPointer maxKey,
            final Comparable<? super K> maxCmp, final boolean maxIncl, final boolean descending) {
            this.m = m;
            this.minKey = minKey;
            this.minCmp = minCmp;
            this.minIncl = minIncl;
            this.maxKey = maxKey;
            this.maxCmp = maxCmp;
            this.maxIncl = maxIncl;
            this.descending = descending;
        }

        /**
         * @param key Key.
         * @return {@code true} If key is too low.
         */
        private boolean tooLow(final K key) {
            if (minCmp == null) {
                return false;
            }
            else {
                final int c = minCmp.compareTo(key);

                return c > 0 || (c == 0 && !minIncl);
            }
        }

        /**
         * @param key Key.
         * @return {@code true} If key is too high.
         */
        private boolean tooHigh(final K key) {
            if (maxCmp == null) {
                return false;
            }
            else {
                final int c = maxCmp.compareTo(key);

                return c < 0 || (c == 0 && !maxIncl);
            }
        }

        /**
         * @param key Key.
         * @return {@code true} If key is in range.
         */
        private boolean inRange(final K key) {
            return !tooLow(key) && !tooHigh(key);
        }

        /**
         * @param key Key.
         */
        private void requireInRange(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!inRange(key)) {
                throw new IllegalArgumentException();
            }
        }

        /**
         * @return Direction for lower side.
         */
        private char minDir() {
            return descending ? Right : Left;
        }

        /**
         * @return Direction for higher side.
         */
        private char maxDir() {
            return descending ? Left : Right;
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, true, Left) == null;
        }

        /** {@inheritDoc} */
        @Override public int size() {
            final long root = right(m.holderRef);

            return computeFrozenSize(root, minCmp, minIncl, maxCmp, maxIncl);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(final Object key) {
            if (key == null) {
                throw new NullPointerException();
            }

            final K k = (K) key;

            return inRange(k) && m.containsKey(k);
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(final Object value) {
            // apply the same null policy as the rest of the code, but fall
            // back to the default implementation
            return super.containsValue(value);
        }

        /** {@inheritDoc} */
        @Override public V get(final Object key) {
            if (key == null) {
                throw new NullPointerException();
            }

            final K k = (K) key;

            return !inRange(k) ? null : m.get(k);
        }

        /** {@inheritDoc} */
        @Override public V put(final K key, final V value) {
            requireInRange(key);

            return m.put(key, value);
        }

        /** {@inheritDoc} */
        @Override public V remove(final Object key) {
            if (key == null) {
                throw new NullPointerException();
            }

            return !inRange((K)key) ? null : m.remove(key);
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<K,V>> entrySet() {
            return new EntrySubSet();
        }

        /**
         * Entry subset.
         */
        private class EntrySubSet extends AbstractSet<Map.Entry<K,V>> {
            /** {@inheritDoc} */
            @Override public int size() {
                return SubMap.this.size();
            }

            /** {@inheritDoc} */
            @Override public boolean isEmpty() {
                return SubMap.this.isEmpty();
            }

            /** {@inheritDoc} */
            @Override public boolean contains(final Object o) {
                if (!(o instanceof Map.Entry<?,?>)) {
                    return false;
                }

                final Object k = ((Map.Entry<?,?>)o).getKey();

                if (!inRange((K)k)) {
                    return false;
                }

                final Object v = ((Map.Entry<?,?>)o).getValue();
                final Object actual = m.getImpl((K)k);

                if (actual == null) {
                    // no associated value
                    return false;
                }

                return v.equals(actual);
            }

            /** {@inheritDoc} */
            @Override public boolean add(final Entry<K,V> e) {
                requireInRange(e.getKey());

                final V v = e.getValue();

                if (v == null)
                    throw new NullPointerException();

                return !v.equals(m.update(e.getKey(), UpdateAlways, null, v));
            }

            /** {@inheritDoc} */
            @Override public boolean remove(final Object o) {
                if (!(o instanceof Map.Entry<?,?>)) {
                    return false;
                }

                final Object k = ((Map.Entry<?,?>)o).getKey();
                final Object v = ((Map.Entry<?,?>)o).getValue();

                return SubMap.this.remove(k, v);
            }

            /** {@inheritDoc} */
            @Override public Iterator<Entry<K,V>> iterator() {
                return new EntryIter(m, minCmp, minIncl, maxCmp, maxIncl, descending);
            }
        }

        //////// SortedMap

        /** {@inheritDoc} */
        @Override public Comparator<? super K> comparator() {
            final Comparator<? super K> fromM = m.comparator();

            if (descending) {
                return Collections.reverseOrder(fromM);
            }
            else {
                return fromM;
            }
        }

        /** {@inheritDoc} */
        @Override public K firstKey() {
            return m.boundedExtremeKeyOrThrow(minCmp, minIncl, maxCmp, maxIncl, minDir());
        }

        /** {@inheritDoc} */
        @Override public K lastKey() {
            return m.boundedExtremeKeyOrThrow(minCmp, minIncl, maxCmp, maxIncl, maxDir());
        }

        /**
         * @return First key or {@code null}.
         */
        private K firstKeyOrNull() {
            return (K) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, true, minDir());
        }

        /**
         * @return Last key or {@code null}.
         */
        private K lastKeyOrNull() {
            return (K) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, true, maxDir());
        }

        /**
         * @return First entry or {@code null}.
         */
        private Entry firstEntryOrNull() {
            return (Entry) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, false, minDir());
        }

        /**
         * @return Last entry or {@code null}.
         */
        private Entry lastEntryOrNull() {
            return (Entry) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, false, maxDir());
        }

        /** {@inheritDoc} */
        @Override public Entry lowerEntry(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooLow(key) : tooHigh(key)) {
                return null;
            }

            return ((!descending ? tooHigh(key) : tooLow(key)) ? this : subMapInRange(null, false, key, false)).
                lastEntryOrNull();
        }

        /** {@inheritDoc} */
        @Override public K lowerKey(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooLow(key) : tooHigh(key)) {
                return null;
            }

            return ((!descending ? tooHigh(key) : tooLow(key)) ? this : subMapInRange(null, false, key, false)).
                lastKeyOrNull();
        }

        /** {@inheritDoc} */
        @Override public Entry floorEntry(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooLow(key) : tooHigh(key)) {
                return null;
            }

            return ((!descending ? tooHigh(key) : tooLow(key)) ? this : subMapInRange(null, false, key, true)).
                    lastEntryOrNull();
        }

        /** {@inheritDoc} */
        @Override public K floorKey(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooLow(key) : tooHigh(key)) {
                return null;
            }

            return ((!descending ? tooHigh(key) : tooLow(key)) ? this : subMapInRange(null, false, key, true)).
                lastKeyOrNull();
        }

        /** {@inheritDoc} */
        @Override public Entry ceilingEntry(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooHigh(key) : tooLow(key)) {
                return null;
            }

            return ((!descending ? tooLow(key) : tooHigh(key)) ? this : subMapInRange(key, true, null, false)).
                firstEntryOrNull();
        }

        /** {@inheritDoc} */
        @Override public K ceilingKey(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooHigh(key) : tooLow(key)) {
                return null;
            }

            return ((!descending ? tooLow(key) : tooHigh(key)) ? this : subMapInRange(key, true, null, false)).
                firstKeyOrNull();
        }

        /** {@inheritDoc} */
        @Override public Entry higherEntry(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooHigh(key) : tooLow(key)) {
                return null;
            }

            return ((!descending ? tooLow(key) : tooHigh(key)) ? this : subMapInRange(key, false, null, false)).
                firstEntryOrNull();
        }

        /** {@inheritDoc} */
        @Override public K higherKey(final K key) {
            if (key == null) {
                throw new NullPointerException();
            }

            if (!descending ? tooHigh(key) : tooLow(key)) {
                return null;
            }

            return ((!descending ? tooLow(key) : tooHigh(key)) ? this : subMapInRange(key, false, null, false)).
                firstKeyOrNull();
        }

        /** {@inheritDoc} */
        @Override public Entry firstEntry() {
            return (Entry) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, false, minDir());
        }

        /** {@inheritDoc} */
        @Override public Entry lastEntry() {
            return (Entry) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, false, maxDir());
        }

        /** {@inheritDoc} */
        @Override public Entry pollFirstEntry() {
            while (true) {
                final Entry snapshot = (Entry) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, false, minDir());

                if (snapshot == null || m.remove(snapshot.getKey(), snapshot.getValue())) {
                    return snapshot;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public Entry pollLastEntry() {
            while (true) {
                final Entry snapshot = (Entry) m.boundedExtreme(minCmp, minIncl, maxCmp, maxIncl, false, maxDir());

                if (snapshot == null || m.remove(snapshot.getKey(), snapshot.getValue())) {
                    return snapshot;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public V putIfAbsent(final K key, final V value) {
            requireInRange(key);

            return m.putIfAbsent(key, value);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(final Object key, final Object value) {
            return inRange((K)key) && m.remove(key, value);
        }

        /** {@inheritDoc} */
        @Override public boolean replace(final K key, final V oldValue, final V newValue) {
            requireInRange(key);

            return m.replace(key, oldValue, newValue);
        }

        /** {@inheritDoc} */
        @Override public V replace(final K key, final V value) {
            requireInRange(key);

            return m.replace(key, value);
        }

        /** {@inheritDoc} */
        @Override public SubMap subMap(final K fromKey, final boolean fromInclusive, final K toKey,
            final boolean toInclusive) {
            if (fromKey == null || toKey == null) {
                throw new NullPointerException();
            }

            return subMapImpl(fromKey, fromInclusive, toKey, toInclusive);
        }

        /** {@inheritDoc} */
        @Override public SubMap headMap(final K toKey, final boolean inclusive) {
            if (toKey == null) {
                throw new NullPointerException();
            }

            return subMapImpl(null, false, toKey, inclusive);
        }

        /** {@inheritDoc} */
        @Override public SubMap tailMap(final K fromKey, final boolean inclusive) {
            if (fromKey == null) {
                throw new NullPointerException();
            }

            return subMapImpl(fromKey, inclusive, null, false);
        }

        /** {@inheritDoc} */
        @Override public SubMap subMap(final K fromKey, final K toKey) {
            return subMap(fromKey, true, toKey, false);
        }

        /** {@inheritDoc} */
        @Override public SubMap headMap(final K toKey) {
            return headMap(toKey, false);
        }

        /** {@inheritDoc} */
        @Override public SubMap tailMap(final K fromKey) {
            return tailMap(fromKey, true);
        }

        /**
         * @param fromKey Lower bound.
         * @param fromIncl Include lower bound.
         * @param toKey Upper bound.
         * @param toIncl Include upper bound.
         * @return Submap.
         */
        private SubMap subMapImpl(final K fromKey, final boolean fromIncl, final K toKey, final boolean toIncl) {
            if (fromKey != null) {
                requireInRange(fromKey);
            }

            if (toKey != null) {
                requireInRange(toKey);
            }

            return subMapInRange(fromKey, fromIncl, toKey, toIncl);
        }

        /**
         * @param fromKey Lower bound.
         * @param fromIncl Include lower bound.
         * @param toKey Upper bound.
         * @param toIncl Include upper bound.
         * @return Submap.
         */
        private SubMap subMapInRange(final K fromKey, final boolean fromIncl, final K toKey, final boolean toIncl) {
            final Comparable<? super K> fromCmp = fromKey == null ? null : m.comparable(fromKey);

            final Comparable<? super K> toCmp = toKey == null ? null : m.comparable(toKey);

            if (fromKey != null && toKey != null) {
                final int c = fromCmp.compareTo(toKey);

                if ((!descending ? c > 0 : c < 0)) {
                    throw new IllegalArgumentException();
                }
            }

            GridOffHeapSmartPointer minK = minKey;
            Comparable<? super K> minC = minCmp;

            boolean minI = minIncl;
            GridOffHeapSmartPointer maxK = maxKey;
            Comparable<? super K> maxC = maxCmp;
            boolean maxI = maxIncl;

            if (fromKey != null) {
                if (!descending) {
                    minK = fromKey;
                    minC = fromCmp;
                    minI = fromIncl;
                }
                else {
                    maxK = fromKey;
                    maxC = fromCmp;
                    maxI = fromIncl;
                }
            }

            if (toKey != null) {
                if (!descending) {
                    maxK = toKey;
                    maxC = toCmp;
                    maxI = toIncl;
                }
                else {
                    minK = toKey;
                    minC = toCmp;
                    minI = toIncl;
                }
            }

            return new SubMap(m, minK, minC, minI, maxK, maxC, maxI, descending);
        }

        /** {@inheritDoc} */
        @Override public SubMap descendingMap() {
            return new SubMap(m, minKey, minCmp, minIncl, maxKey, maxCmp, maxIncl, !descending);
        }

        /** {@inheritDoc} */
        @Override public NavigableSet<K> keySet() {
            return navigableKeySet();
        }

        /** {@inheritDoc} */
        @Override public NavigableSet<K> navigableKeySet() {
            return new KeySet(SubMap.this) {
                public Iterator<GridOffHeapSmartPointer> iterator() {
                    return new KeyIter(m, minCmp, minIncl, maxCmp, maxIncl, descending);
                }
            };
        }

        /** {@inheritDoc} */
        @Override public NavigableSet<K> descendingKeySet() {
            return descendingMap().navigableKeySet();
        }
    }

    /**
     * Key lock for pointer locking.
     */
    static class KeyLock {
        /** */
        private final ConcurrentHashMap8<Object, Lock> m = new ConcurrentHashMap8<Object, Lock>();

        /**
         * Locks given key. Key is required to correctly implement {@link Object#hashCode()} and
         * {@link Object#equals(Object)} methods.
         *
         * @param key Key.
         * @return Taken lock.
         */
        @Nullable
        public Lock lock(Object key) {
            Thread th = Thread.currentThread();

            Lock l = new Lock(key, th);

            for (;;) {
                Lock l2 = m.putIfAbsent(key, l);

                if (l2 == null)
                    return l;

                if (l2.owner == th)
                    return null;

                try {
                    l2.await();
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        /**
         * Lock for key.
         */
        @SuppressWarnings("PublicInnerClass")
        public class Lock {
            /** */
            private final Object key;

            /** */
            private final Thread owner;

            /** */
            private final CountDownLatch latch = new CountDownLatch(1);

            /**
             * Constructor.
             *
             * @param key Key.
             * @param owner Owner.
             */
            private Lock(Object key, Thread owner) {
                this.key = key;
                this.owner = owner;
            }

            /**
             * Awaits until lock will be unlocked.
             *
             * @throws InterruptedException if thread was interrupted.
             */
            private void await() throws InterruptedException {
                latch.await();
            }

            /**
             * Unlock this lock.
             */
            public void unlock() {
                Lock l = m.remove(key);

                assert owner == Thread.currentThread();
                assert l == this;

                latch.countDown();
            }
        }
    }
}


