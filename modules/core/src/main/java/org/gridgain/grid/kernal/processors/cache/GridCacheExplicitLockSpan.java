/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Collection of near local locks acquired by a thread on one topology version.
 */
public class GridCacheExplicitLockSpan<K> extends ReentrantLock {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology snapshot. */
    @GridToStringInclude
    private final GridDiscoveryTopologySnapshot topSnapshot;

    /** Pending candidates. */
    @GridToStringInclude
    private final Map<K, Deque<GridCacheMvccCandidate<K>>> cands = new HashMap<>();

    /** Span lock release future. */
    @GridToStringInclude
    private final GridFutureAdapter<Object> releaseFut = new GridFutureAdapter<>();

    /**
     * @param topSnapshot Topology snapshot.
     * @param cand Candidate.
     */
    public GridCacheExplicitLockSpan(GridDiscoveryTopologySnapshot topSnapshot, GridCacheMvccCandidate<K> cand) {
        this.topSnapshot = topSnapshot;

        ensureDeque(cand.key()).addFirst(cand);
    }

    /**
     * Adds candidate to a lock span.
     *
     * @param topSnapshot Topology snapshot for which candidate is added.
     * @param cand Candidate to add.
     * @return {@code True} if candidate was added, {@code false} if this span is empty and
     *      new span should be created.
     */
    public boolean addCandidate(GridDiscoveryTopologySnapshot topSnapshot, GridCacheMvccCandidate<K> cand) {
        lock();

        try {
            if (cands.isEmpty())
                return false;

            assert this.topSnapshot.topologyVersion() == topSnapshot.topologyVersion();

            Deque<GridCacheMvccCandidate<K>> deque = ensureDeque(cand.key());

            GridCacheMvccCandidate<K> old = F.first(deque);

            deque.add(cand);

            if (old != null && old.owner())
                cand.setOwner();

            return true;
        }
        finally {
            unlock();
        }
    }

    /**
     * Removes candidate from this lock span.
     *
     * @param cand Candidate to remove.
     * @return {@code True} if span is empty and should be removed, {@code false} otherwise.
     */
    public boolean removeCandidate(GridCacheMvccCandidate<K> cand) {
        lock();

        try {
            Deque<GridCacheMvccCandidate<K>> deque = cands.get(cand.key());

            if (deque != null) {
                assert !deque.isEmpty();

                if (deque.peekFirst().equals(cand)) {
                    deque.removeFirst();

                    if (deque.isEmpty())
                        cands.remove(cand.key());
                }
            }

            boolean empty = cands.isEmpty();

            if (empty)
                releaseFut.onDone();

            return empty;
        }
        finally {
            unlock();
        }
    }

    /**
     * Removes lock by key and optional version.
     *
     * @param key Key.
     * @param ver Version (or {@code null} if any candidate should be removed.)
     * @return Removed candidate if matches given parameters.
     */
    public GridCacheMvccCandidate<K> removeCandidate(K key, @Nullable GridCacheVersion ver) {
        lock();

        try {
            Deque<GridCacheMvccCandidate<K>> deque = cands.get(key);

            GridCacheMvccCandidate<K> cand = null;

            if (deque != null) {
                assert !deque.isEmpty();

                if (ver == null || deque.peekFirst().version().equals(ver)) {
                    cand = deque.removeFirst();

                    if (deque.isEmpty())
                        cands.remove(cand.key());
                }
            }

            boolean empty = cands.isEmpty();

            if (empty)
                releaseFut.onDone();

            return cand;
        }
        finally {
            unlock();
        }
    }

    /**
     * @return {@code True} if span is empty and candidates cannot be added anymore.
     */
    public boolean isEmpty() {
        lock();

        try {
            return cands.isEmpty();
        }
        finally {
            unlock();
        }
    }

    /**
     * Marks all candidates added for given key as owned.
     *
     * @param key Key.
     */
    public void markOwned(K key) {
        lock();

        try {
            Deque<GridCacheMvccCandidate<K>> deque = cands.get(key);

            assert deque != null;

            for (GridCacheMvccCandidate<K> cand : deque)
                cand.setOwner();
        }
        finally {
            unlock();
        }
    }

    /**
     * Gets explicit lock candidate for given key.
     *
     * @param key Key to lookup.
     * @param ver Version to lookup (if {@code null} - return any).
     * @return Last added explicit lock candidate, if any, or {@code null}.
     */
    @Nullable public GridCacheMvccCandidate<K> candidate(K key, @Nullable final GridCacheVersion ver) {
        lock();

        try {
            Deque<GridCacheMvccCandidate<K>> deque = cands.get(key);

            if (deque != null) {
                assert !deque.isEmpty();

                return ver == null ? deque.peekFirst() : F.find(deque, null, new P1<GridCacheMvccCandidate<K>>() {
                    @Override public boolean apply(GridCacheMvccCandidate<K> cand) {
                        return cand.version().equals(ver);
                    }
                });
            }

            return null;
        }
        finally {
            unlock();
        }
    }

    /**
     * Gets actual topology snapshot for thread lock span.
     *
     * @return Topology snapshot or {@code null} if candidate list is empty.
     */
    @Nullable public GridDiscoveryTopologySnapshot topologySnapshot() {
        return releaseFut.isDone() ? null : topSnapshot;
    }

    /**
     * Gets span release future. Future is completed when last lock is released.
     *
     * @return Release future.
     */
    public IgniteFuture<Object> releaseFuture() {
        return releaseFut;
    }

    /**
     * Gets deque from candidate map and adds it if it does not exist.
     *
     * @param key Key to look up.
     * @return Deque.
     */
    private Deque<GridCacheMvccCandidate<K>> ensureDeque(K key) {
        Deque<GridCacheMvccCandidate<K>> deque = cands.get(key);

        if (deque == null) {
            deque = new LinkedList<>();

            cands.put(key, deque);
        }

        return deque;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        lock();

        try {
            return S.toString(GridCacheExplicitLockSpan.class, this);
        }
        finally {
            unlock();
        }
    }
}
