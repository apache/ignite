/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * Class RootReference is an immutable structure to represent state of the MVMap as a whole
 * (not related to a particular B-Tree node).
 * Single structure would allow for non-blocking atomic state change.
 * The most important part of it is a reference to the root node.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class RootReference
{
    /**
     * The root page.
     */
    public final Page root;
    /**
     * The version used for writing.
     */
    public final long version;
    /**
     * Indicator that map is locked for update.
     */
    final boolean lockedForUpdate;
    /**
     * Reference to the previous root in the chain.
     */
    public volatile RootReference previous;
    /**
     * Counter for successful root updates.
     */
    final long updateCounter;
    /**
     * Counter for attempted root updates.
     */
    final long updateAttemptCounter;
    /**
     * Size of the occupied part of the append buffer.
     */
    final byte appendCounter;

    // This one is used to set root initially and for r/o snapshots
    RootReference(Page root, long version) {
        this.root = root;
        this.version = version;
        this.previous = null;
        this.updateCounter = 1;
        this.updateAttemptCounter = 1;
        this.lockedForUpdate = false;
        this.appendCounter = 0;
    }

    RootReference(RootReference r, Page root, long updateAttemptCounter) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + updateAttemptCounter;
        this.lockedForUpdate = false;
        this.appendCounter = r.appendCounter;
    }

    // This one is used for locking
    RootReference(RootReference r, int attempt) {
        this.root = r.root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        this.lockedForUpdate = true;
        this.appendCounter = r.appendCounter;
    }

    // This one is used for unlocking
    RootReference(RootReference r, Page root, int appendCounter, boolean lockedForUpdate) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter;
        this.updateAttemptCounter = r.updateAttemptCounter;
        this.lockedForUpdate = lockedForUpdate;
        this.appendCounter = (byte) appendCounter;
    }

    // This one is used for version change
    RootReference(RootReference r, long version, int attempt) {
        RootReference previous = r;
        RootReference tmp;
        while ((tmp = previous.previous) != null && tmp.root == r.root) {
            previous = tmp;
        }
        this.root = r.root;
        this.version = version;
        this.previous = previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        this.lockedForUpdate = r.lockedForUpdate;
        this.appendCounter = r.appendCounter;
    }

    int getAppendCounter() {
        return appendCounter & 0xff;
    }

    public long getTotalCount() {
        return root.getTotalCount() + getAppendCounter();
    }

    @Override
    public String toString() {
        return "RootReference(" + System.identityHashCode(root) + "," + version + "," + lockedForUpdate + ")";
    }
}
