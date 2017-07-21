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

package org.apache.ignite.internal.util.snaptree;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/** Provides an implementation of the behavior of an {@link Epoch}. */
@SuppressWarnings("ALL")
abstract class EpochNode extends AtomicLong implements Epoch.Ticket {

    private static final int TRIES_BEFORE_SUBTREE = 2;
    private static final int CLOSER_HEAD_START = 1000;

    /** This includes the root.  7 or fewer procs gets 2, 63 or fewer gets
     *  3, 511 or fewer 4.  We observe that the node count reported by {@link
     *  #computeSpread} is roughly twice the number of hardware contexts in
     *  use.
     */
    private static final int MAX_LEVELS = 2 + log8(Runtime.getRuntime().availableProcessors());

    /** Returns floor(log_base_8(value)). */
    private static int log8(final int value) {
        return (31 - Integer.numberOfLeadingZeros(value)) / 3;
    }

    //////////////// branching factor

    private static final int LOG_BF = 3;
    private static final int BF = 1 << LOG_BF;
    private static final int BF_MASK = BF - 1;

    //////////////// bit packing

    private static final int DATA_SUM_SHIFT = 32;
    private static int dataSum(long state) { return (int)(state >> DATA_SUM_SHIFT); }
    private static long withDataDelta(long state, int delta) { return state + (((long) delta) << DATA_SUM_SHIFT); }

    private static final int CHILD_CLOSED_SHIFT = 32 - BF;
    private static long ALL_CHILDREN_CLOSED = ((1L << BF) - 1L) << CHILD_CLOSED_SHIFT;
    private static long childClosedBit(int which) { return 1L << (CHILD_CLOSED_SHIFT + which); }
    private static boolean isChildClosed(long state, int which) { return (state & childClosedBit(which)) != 0; }
    private static long withChildClosed(long state, int which, long childState) {
        assert(!isChildClosed(state, which));
        return withDataDelta(state | childClosedBit(which), dataSum(childState));
    }
    private static boolean isAllChildrenClosed(long state) { return (state & ALL_CHILDREN_CLOSED) == ALL_CHILDREN_CLOSED; }

    private static final int CHILD_PRESENT_SHIFT = CHILD_CLOSED_SHIFT - BF;
    private static final long ANY_CHILD_PRESENT = ((1L << BF) - 1L) << CHILD_PRESENT_SHIFT;
    private static long childPresentBit(int which) { return 1L << (CHILD_PRESENT_SHIFT + which); }
    private static boolean isChildPresent(long state, int which) { return (state & childPresentBit(which)) != 0; }
    private static long withChildPresent(long state, int which) { return state | childPresentBit(which); }
    private static boolean isAnyChildPresent(long state) { return (state & ANY_CHILD_PRESENT) != 0; }

    private static final long MARK = (1L << (CHILD_PRESENT_SHIFT - 1));
    private static boolean isMarked(long state) { return (state & MARK) != 0L; }
    /** Records all non-present children as closed. */
    private static long withMarked(long state) {
        final int missingChildren = (~((int) state) >> CHILD_PRESENT_SHIFT) & ((1 << BF) - 1);
        return state | MARK | (((long) missingChildren) << CHILD_CLOSED_SHIFT);
    }

    private static final long ENTRY_COUNT_MASK = MARK - 1;
    private static int entryCount(long state) { return (int) (state & ENTRY_COUNT_MASK); }
    private static long withArrive(long state) { return state + 1; }
    private static long withLeave(long state, int dataDelta) { return withDataDelta(state - 1, dataDelta); }
    private static boolean mayArrive(long state) { return entryCount(state) != ENTRY_COUNT_MASK; }
    private static boolean mayLeave(long state) { return entryCount(state) != 0; }

    private static final long CLOSED_MASK = MARK | ALL_CHILDREN_CLOSED | ENTRY_COUNT_MASK;
    private static final long CLOSED_VALUE = MARK | ALL_CHILDREN_CLOSED;
    private static boolean isClosed(long state) { return (state & CLOSED_MASK) == CLOSED_VALUE; }

    private static final long ENTRY_FAST_PATH_MASK = ANY_CHILD_PRESENT | MARK | (ENTRY_COUNT_MASK - (ENTRY_COUNT_MASK >> 1));
    /** Not marked, no children, and no overflow possible. */
    private static boolean isEntryFastPath(long state) { return (state & ENTRY_FAST_PATH_MASK) == 0L; }

    //////////////// subclasses

    private static class Child extends EpochNode {
        /** */
        private static final long serialVersionUID = 0L;

        private Child(final EpochNode parent, final int whichInParent) {
            super(parent, whichInParent);
        }

        protected void onClosed(final int dataSum) {
            throw new Error();
        }
    }

    //////////////// instance state

    private static final AtomicReferenceFieldUpdater[] childrenUpdaters = {
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child0"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child1"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child2"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child3"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child4"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child5"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child6"),
        AtomicReferenceFieldUpdater.newUpdater(EpochNode.class, EpochNode.class, "_child7")
    };

    private final EpochNode _parent;
    private final int _whichInParent;

    // It would be cleaner to use an array of children, but we want to force
    // all of the bulk into the same object as the AtomicLong.value.

    // To avoid races between creating a child and marking a node as closed,
    // we add a bit to the state for each child that records whether it
    // *should* exist.  If we find that the bit is set but a child is missing,
    // we can create it ourself.

    private volatile EpochNode _child0;
    private volatile EpochNode _child1;
    private volatile EpochNode _child2;
    private volatile EpochNode _child3;
    private volatile EpochNode _child4;
    private volatile EpochNode _child5;
    private volatile EpochNode _child6;
    private volatile EpochNode _child7;

    EpochNode() {
        _parent = null;
        _whichInParent = 0;
    }

    private EpochNode(final EpochNode parent, final int whichInParent) {
        _parent = parent;
        _whichInParent = whichInParent;
    }

    //////////////// provided by the caller

    abstract protected void onClosed(int dataSum);

    //////////////// child management

    private EpochNode getChildFromField(final int which) {
        switch (which) {
            case 0: return _child0;
            case 1: return _child1;
            case 2: return _child2;
            case 3: return _child3;
            case 4: return _child4;
            case 5: return _child5;
            case 6: return _child6;
            case 7: return _child7;
            default: return null;
        }
    }

    private EpochNode getChild(final long state, final int which) {
        if (!isChildPresent(state, which)) {
            return null;
        }
        final EpochNode existing = getChildFromField(which);
        return (existing != null) ? existing : constructPresentChild(which);
    }

    @SuppressWarnings("unchecked")
    private EpochNode constructPresentChild(final int which) {
        final EpochNode n = new Child(this, which);
        return childrenUpdaters[which].compareAndSet(this, null, n) ? n : getChildFromField(which);
    }

    private EpochNode getOrCreateChild(final int which) {
        final EpochNode existing = getChildFromField(which);
        return (existing != null) ? existing : createChild(which);
    }

    private EpochNode createChild(final int which) {
        while (true) {
            final long state = get();
            if (isMarked(state)) {
                // whatever we've got is what we've got
                return getChild(state, which);
            }
            if (compareAndSet(state, withChildPresent(state, which))) {
                // the child now should exist, but we must still actually
                // construct and link in the instance
                return constructPresentChild(which);
            }
        }
    }

    /** Returns the <code>Node</code> to decr on success, null if
     *  {@link #beginClose} has already been called on this instance.
     */
    public EpochNode attemptArrive() {
        final long state = get();
        if (isEntryFastPath(state) && compareAndSet(state, withArrive(state))) {
            return this;
        }
        else {
            return attemptArrive(0, 1);
        }
    }

    private int getIdentity() {
        final int h = System.identityHashCode(Thread.currentThread());

        // Multiply by -127, as suggested by java.util.IdentityHashMap.
        // We also set an bit we don't use, to make sure it is never zero.
        return (h - (h << 7)) | (1 << 31);
    }

    /** level 1 is the root. */
    private EpochNode attemptArrive(int id, final int level) {
        int tries = 0;
        while (true) {
            final long state = get();
            if (isMarked(state)) {
                return null;
            }
            if (isAnyChildPresent(state) ||
                    (tries >= TRIES_BEFORE_SUBTREE && level < MAX_LEVELS)) {
                // Go deeper if we have previously detected contention, or if
                // we are currently detecting it.  Lazy computation of our
                // current identity.
                if (id == 0) {
                    id = getIdentity();
                }
                final EpochNode child = getOrCreateChild(id & BF_MASK);
                if (child == null) {
                    return null;
                }
                return child.attemptArrive(id >> LOG_BF, level + 1);
            }
            if (!mayArrive(state)) {
                throw new IllegalStateException("maximum arrival count of " + ENTRY_COUNT_MASK + " exceeded");
            }
            if (compareAndSet(state, withArrive(state))) {
                // success
                return this;
            }

            ++tries;
        }
    }

    /** Should be called on every non-null return value from attemptArrive. */
    public void leave(final int dataDelta) {
        while (true) {
            final long state = get();
            if (!mayLeave(state)) {
                throw new IllegalStateException("incorrect call to Epoch.leave");
            }
            final long after = withLeave(state, dataDelta);
            if (compareAndSet(state, after)) {
                if (isClosed(after)) {
                    newlyClosed(after);
                }
                return;
            }
        }
    }

    private void newlyClosed(final long state) {
        if (_parent != null) {
            // propogate
            _parent.childIsNowClosed(_whichInParent, state);
        }
        else {
            // report
            onClosed(dataSum(state));
        }
    }

    private void childIsNowClosed(final int which, final long childState) {
        while (true) {
            final long state = get();
            if (isChildClosed(state, which)) {
                // not our problem
                return;
            }
            final long after = withChildClosed(state, which, childState);
            if (compareAndSet(state, after)) {
                if (isClosed(after)) {
                    newlyClosed(after);
                }
                return;
            }
        }
    }

    /** Prevents subsequent calls to {@link #attemptArrive} from succeeding. */
    public void beginClose() {
        int attempts = 0;
        long state;
        while (true) {
            ++attempts;

            state = get();
            if (isClosed(state)) {
                return;
            }

            if (isMarked(state)) {
                // give the thread that actually performed this transition a
                // bit of a head start
                if (attempts < CLOSER_HEAD_START) {
                    continue;
                }
                break;
            }

            // every child that is not present will be recorded as closed by withMarked
            final long after = withMarked(state);
            if (compareAndSet(state, after)) {
                if (isAllChildrenClosed(after)) {
                    if (isClosed(after) && _parent == null) {
                        // finished in one CAS, yeah!
                        onClosed(dataSum(after));
                    }
                    // no second stage necessary
                    return;
                }
                // CAS successful, so now we need to beginClose() the children
                break;
            }
        }

        // no new child bits can be set after marking, so this gets everyone
        for (int which = 0; which < BF; ++which) {
            final EpochNode child = getChild(state, which);
            if (child != null) {
                child.beginClose();
            }
        }

        // Rather than have each child bubble up its closure, we gather it
        // here to reduce the number of CASs required.
        while (true) {
            final long before = get();
            long after = before;
            for (int which = 0; which < BF; ++which) {
                if (!isChildClosed(before, which)) {
                    final long childState = getChildFromField(which).get();
                    if (isClosed(childState)) {
                        after = withChildClosed(after, which, childState);
                    }
                }
            }
            if (before == after) {
                return;
            }
            if (compareAndSet(before, after)) {
                if (isClosed(after) && _parent == null) {
                    onClosed(dataSum(after));
                }
                return;
            }
        }
    }

    /** If possible returns the <code>dataSum</code> that would be delivered
     *  to {@link #onClosed(int)} if this epoch were closed at this moment,
     *  otherwise returns null.  This will succeed if and only if the tree
     *  consists only of a single node.
     */
    public Integer attemptDataSum() {
        final long state = get();
        if (!isAnyChildPresent(state) && entryCount(state) == 0) {
            // this is better than Integer.valueOf for dynamic escape analysis
            //return new Integer(dataSum(state));
            // this is better than new Integer() for object creation
            return Integer.valueOf(dataSum(state));
        }
        else {
            return null;
        }
    }

    /** For debugging purposes. */
    int computeSpread() {
        final long state = get();
        if (isAnyChildPresent(state)) {
            int sum = 0;
            for (int which = 0; which < BF; ++which) {
                final EpochNode child = getChild(state, which);
                if (child != null) {
                    sum += child.computeSpread();
                }
                else {
                    // child would be created for arrive, so count it
                    sum += 1;
                }
            }
            return sum;
        }
        else {
            return 1;
        }
    }
}