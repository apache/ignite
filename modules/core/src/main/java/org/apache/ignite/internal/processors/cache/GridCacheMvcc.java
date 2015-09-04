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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Replicated lock based on MVCC paradigm. This class ensures that locks are acquired
 * in proper order and that there is no more than only one active lock present at all
 * times. It also ensures that new generated lock candidates will appear after
 * old ones in the pending set, hence preventing lock starvation.
 * See {@link GridCacheVersionManager#next()} for information on how lock IDs are
 * generated to prevent starvation.
 */
public final class GridCacheMvcc {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static volatile IgniteLogger log;

    /** Cache context. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Local queue. */
    @GridToStringInclude
    private LinkedList<GridCacheMvccCandidate> locs;

    /** Remote queue. */
    @GridToStringInclude
    private LinkedList<GridCacheMvccCandidate> rmts;

    /**
     * @param cctx Cache context.
     */
    public GridCacheMvcc(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheMvcc.class);
    }

    /**
     * @return Any owner.
     */
    @Nullable public GridCacheMvccCandidate anyOwner() {
        GridCacheMvccCandidate owner = localOwner();

        if (owner == null)
            owner = remoteOwner();

        return owner;
    }

    /**
     * @return Remote candidate only if it's first in the list and is marked
     *      as <tt>'used'</tt>.
     */
    @Nullable public GridCacheMvccCandidate remoteOwner() {
        if (rmts != null) {
            assert !rmts.isEmpty();

            GridCacheMvccCandidate first = rmts.getFirst();

            return first.used() && first.owner() ? first : null;
        }

        return null;
    }

    /**
     * @return Local candidate only if it's first in the list and is marked
     *      as <tt>'owner'</tt>.
     */
    @Nullable public GridCacheMvccCandidate localOwner() {
        if (locs != null) {
            assert !locs.isEmpty();

            GridCacheMvccCandidate first = locs.getFirst();

            return first.owner() ? first : null;
        }

        return null;
    }

    /**
     * @param cands Candidates to search.
     * @param ver Version.
     * @return Candidate for the version.
     */
    @Nullable private GridCacheMvccCandidate candidate(Iterable<GridCacheMvccCandidate> cands,
        GridCacheVersion ver) {
        assert ver != null;

        if (cands != null)
            for (GridCacheMvccCandidate c : cands)
                if (c.version().equals(ver))
                    return c;

        return null;
    }

    /**
     *
     * @param threadId Thread ID.
     * @param reentry Reentry flag.
     * @return Local candidate for the thread.
     */
    @Nullable private GridCacheMvccCandidate localCandidate(long threadId, boolean reentry) {
        if (locs != null)
            for (GridCacheMvccCandidate cand : locs) {
                if (cand.threadId() == threadId) {
                    if (cand.reentry() && !reentry)
                        continue;

                    return cand;
                }
            }

        return null;
    }

    /**
     * @param cand Candidate to add.
     */
    private void add0(GridCacheMvccCandidate cand) {
        assert cand != null;

        // Local.
        if (cand.local()) {
            if (locs == null)
                locs = new LinkedList<>();

            if (!cand.nearLocal()) {
                if (!locs.isEmpty()) {
                    GridCacheMvccCandidate c = locs.getFirst();

                    if (c.owner()) {
                        // If reentry, add at the beginning. Note that
                        // no reentry happens for DHT-local candidates.
                        if (!cand.dhtLocal() && c.threadId() == cand.threadId()) {
                            cand.setOwner();
                            cand.setReady();
                            cand.setReentry();

                            locs.addFirst(cand);

                            return;
                        }
                    }

                    // Iterate in reverse order.
                    for (ListIterator<GridCacheMvccCandidate> it = locs.listIterator(locs.size()); it.hasPrevious(); ) {
                        c = it.previous();

                        assert !c.version().equals(cand.version()) : "Versions can't match [existing=" + c +
                            ", new=" + cand + ']';

                        // Add after the owner.
                        if (c.owner()) {
                            // Threads are checked above.
                            assert cand.dhtLocal() || c.threadId() != cand.threadId();

                            // Reposition.
                            it.next();

                            it.add(cand);

                            return;
                        }

                        // If not the owner, add after the lesser version.
                        if (c.version().isLess(cand.version())) {
                            // Reposition.
                            it.next();

                            it.add(cand);

                            return;
                        }
                    }
                }

                // Either list is empty or candidate is first.
                locs.addFirst(cand);
            }
            else
                // For near local candidates just add it to the end of list.
                locs.add(cand);
        }
        // Remote.
        else {
            if (rmts == null)
                rmts = new LinkedList<>();

            assert !cand.owner() || localOwner() == null : "Cannot have local and remote owners " +
                "at the same time [cand=" + cand + ", locs=" + locs + ", rmts=" + rmts + ']';

            GridCacheMvccCandidate cur = candidate(rmts, cand.version());

            // For existing candidates, we only care about owners and keys.
            if (cur != null) {
                if (cand.owner())
                    cur.setOwner();

                return;
            }

            // Either list is empty or candidate is last.
            rmts.add(cand);
        }
    }

    /**
     * @param ver Version.
     * @param preferLoc Whether or not to prefer local candidates.
     */
    private void remove0(GridCacheVersion ver, boolean preferLoc) {
        if (preferLoc) {
            if (!remove0(locs, ver))
                remove0(rmts, ver);
        }
        else if (!remove0(rmts, ver))
            remove0(locs, ver);

        if (locs != null && locs.isEmpty())
            locs = null;

        if (rmts != null && rmts.isEmpty())
            rmts = null;
    }

    /**
     * Removes candidate from collection.
     *
     * @param col Collection.
     * @param ver Version of the candidate to remove.
     * @return {@code True} if candidate was removed.
     */
    private boolean remove0(Collection<GridCacheMvccCandidate> col, GridCacheVersion ver) {
        if (col != null) {
            for (Iterator<GridCacheMvccCandidate> it = col.iterator(); it.hasNext(); ) {
                GridCacheMvccCandidate cand = it.next();

                if (cand.version().equals(ver)) {
                    cand.setUsed();
                    cand.setRemoved();

                    it.remove();

                    reassign();

                    if (cand.local())
                        cctx.mvcc().removeLocal(cand);

                    return true;
                }
            }
        }

        return false;
    }

    /**
     *
     * @param exclude Versions to exclude form check.
     * @return {@code True} if lock is empty.
     */
    public boolean isEmpty(GridCacheVersion... exclude) {
        if (locs == null && rmts == null)
            return true;

        if (locs != null) {
            assert !locs.isEmpty();

            if (F.isEmpty(exclude))
                return false;

            for (GridCacheMvccCandidate cand : locs)
                if (!U.containsObjectArray(exclude, cand.version()))
                    return false;
        }

        if (rmts != null) {
            assert !rmts.isEmpty();

            if (F.isEmpty(exclude))
                return false;

            for (GridCacheMvccCandidate cand : rmts)
                if (!U.containsObjectArray(exclude, cand.version()))
                    return false;
        }

        return true;
    }

    /**
     * Moves completed candidates right before the base one. Note that
     * if base is not found, then nothing happens and {@code false} is
     * returned.
     *
     * @param baseVer Base version.
     * @param committedVers Committed versions relative to base.
     * @param rolledbackVers Rolled back versions relative to base.
     * @return Lock owner.
     */
    @Nullable public GridCacheMvccCandidate orderCompleted(GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers) {
        assert baseVer != null;

        if (rmts != null && !F.isEmpty(committedVers)) {
            Deque<GridCacheMvccCandidate> mvAfter = null;

            int maxIdx = -1;

            for (ListIterator<GridCacheMvccCandidate> it = rmts.listIterator(rmts.size()); it.hasPrevious(); ) {
                GridCacheMvccCandidate cur = it.previous();

                if (!cur.version().equals(baseVer) && committedVers.contains(cur.version())) {
                    cur.setOwner();

                    assert localOwner() == null || localOwner().nearLocal(): "Cannot not have local owner and " +
                        "remote completed transactions at the same time [baseVer=" + baseVer +
                        ", committedVers=" + committedVers + ", rolledbackVers=" + rolledbackVers +
                        ", localOwner=" + localOwner() + ", locs=" + locs + ", rmts=" + rmts + ']';

                    if (maxIdx < 0)
                        maxIdx = it.nextIndex();
                }
                else if (maxIdx >= 0 && cur.version().isGreaterEqual(baseVer)) {
                    if (--maxIdx >= 0) {
                        if (mvAfter == null)
                            mvAfter = new LinkedList<>();

                        it.remove();

                        mvAfter.addFirst(cur);
                    }
                }

                // If base is completed, then set it to owner too.
                if (!cur.owner() && cur.version().equals(baseVer) && committedVers.contains(cur.version()))
                    cur.setOwner();
            }

            if (maxIdx >= 0 && mvAfter != null) {
                ListIterator<GridCacheMvccCandidate> it = rmts.listIterator(maxIdx + 1);

                for (GridCacheMvccCandidate cand : mvAfter)
                    it.add(cand);
            }

            // Remove rolled back versions.
            if (!F.isEmpty(rolledbackVers)) {
                for (Iterator<GridCacheMvccCandidate> it = rmts.iterator(); it.hasNext(); ) {
                    GridCacheMvccCandidate cand = it.next();

                    if (rolledbackVers.contains(cand.version())) {
                        cand.setUsed(); // Mark as used to be consistent, even though we are about to remove it.

                        it.remove();
                    }
                }

                if (rmts.isEmpty())
                    rmts = null;
            }
        }

        return anyOwner();
    }

    /**
     * Puts owned versions in front of base.
     *
     * @param baseVer Base version.
     * @param owned Owned list.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate markOwned(GridCacheVersion baseVer, GridCacheVersion owned) {
        if (owned == null)
            return anyOwner();

        if (rmts != null) {
            GridCacheMvccCandidate baseCand = candidate(rmts, baseVer);

            if (baseCand != null)
                baseCand.ownerVersion(owned);
        }

        return anyOwner();
    }

    /**
     * @param parent Parent entry.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquisition timeout.
     * @param reenter Reentry flag ({@code true} if reentry is allowed).
     * @param tx Transaction flag.
     * @param implicitSingle Implicit transaction flag.
     * @return New lock candidate if lock was added, or current owner if lock was reentered,
     *      or <tt>null</tt> if lock was owned by another thread and timeout is negative.
     */
    @Nullable public GridCacheMvccCandidate addLocal(
        GridCacheEntryEx parent,
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle) {
        return addLocal(
            parent,
            /*nearNodeId*/null,
            /*nearVer*/null,
            threadId,
            ver,
            timeout,
            reenter,
            tx,
            implicitSingle,
            /*dht-local*/false
        );
    }

    /**
     * @param parent Parent entry.
     * @param nearNodeId Near node ID.
     * @param nearVer Near version.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquisition timeout.
     * @param reenter Reentry flag ({@code true} if reentry is allowed).
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @param dhtLoc DHT local flag.
     * @return New lock candidate if lock was added, or current owner if lock was reentered,
     *      or <tt>null</tt> if lock was owned by another thread and timeout is negative.
     */
    @Nullable public GridCacheMvccCandidate addLocal(
        GridCacheEntryEx parent,
        @Nullable UUID nearNodeId,
        @Nullable GridCacheVersion nearVer,
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean reenter,
        boolean tx,
        boolean implicitSingle,
        boolean dhtLoc) {
        if (log.isDebugEnabled())
            log.debug("Adding local candidate [mvcc=" + this + ", parent=" + parent + ", threadId=" + threadId +
                ", ver=" + ver + ", timeout=" + timeout + ", reenter=" + reenter + ", tx=" + tx + "]");

        // Don't check reenter for DHT candidates.
        if (!dhtLoc && !reenter) {
            GridCacheMvccCandidate owner = localOwner();

            if (owner != null && owner.threadId() == threadId)
                return null;
        }

        // If there are pending locks and timeout is negative,
        // then we give up right away.
        if (timeout < 0) {
            if (locs != null || rmts != null) {
                GridCacheMvccCandidate owner = localOwner();

                // Only proceed if this is a re-entry.
                if (owner == null || owner.threadId() != threadId)
                    return null;
            }
        }

        UUID locNodeId = cctx.nodeId();

        // If this is a reentry, then reentry flag will be flipped within 'add0(..)' method.
        GridCacheMvccCandidate cand = new GridCacheMvccCandidate(
            parent,
            locNodeId,
            nearNodeId,
            nearVer,
            threadId,
            ver,
            timeout,
            /*local*/true,
            /*reenter*/false,
            tx,
            implicitSingle,
            /*near-local*/false, dhtLoc
        );

        cctx.mvcc().addLocal(cand);

        add0(cand);

        return cand;
    }

    /**
     * Adds new remote lock candidate (either near remote or dht remote).
     *
     * @param parent Parent entry.
     * @param nodeId Node ID.
     * @param otherNodeId Other node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquire timeout.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @param nearLoc Near local flag.
     * @return Add remote candidate.
     */
    public GridCacheMvccCandidate addRemote(
        GridCacheEntryEx parent,
        UUID nodeId,
        @Nullable UUID otherNodeId,
        long threadId,
        GridCacheVersion ver,
        long timeout,
        boolean tx,
        boolean implicitSingle,
        boolean nearLoc) {
        GridCacheMvccCandidate cand = new GridCacheMvccCandidate(
            parent,
            nodeId,
            otherNodeId,
            null,
            threadId,
            ver,
            timeout,
            /*local*/false,
            /*reentry*/false,
            tx,
            implicitSingle,
            nearLoc,
            false
        );

        addRemote(cand);

        return cand;
    }

    /**
     * Adds new near local lock candidate.
     *
     * @param parent Parent entry.
     * @param nodeId Node ID.
     * @param otherNodeId Other node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquire timeout.
     * @param tx Transaction flag.
     * @param implicitSingle Implicit flag.
     * @return Add remote candidate.
     */
    public GridCacheMvccCandidate addNearLocal(GridCacheEntryEx parent, UUID nodeId,
        @Nullable UUID otherNodeId, long threadId, GridCacheVersion ver, long timeout, boolean tx,
        boolean implicitSingle) {
        GridCacheMvccCandidate cand = new GridCacheMvccCandidate(parent, nodeId, otherNodeId, null, threadId, ver,
            timeout, /*local*/true, /*reentry*/false, tx, implicitSingle, /*near loc*/true, /*dht loc*/false);

        add0(cand);

        return cand;
    }

    /**
     * @param cand Remote candidate.
     */
    public void addRemote(GridCacheMvccCandidate cand) {
        assert !cand.local();

        if (log.isDebugEnabled())
            log.debug("Adding remote candidate [mvcc=" + this + ", cand=" + cand + "]");

        cctx.versions().onReceived(cand.nodeId(), cand.version());

        add0(cand);
    }

    /**
     * @param ver Lock version to acquire or set to ready.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate readyLocal(GridCacheVersion ver) {
        GridCacheMvccCandidate cand = candidate(ver);

        if (cand == null)
            return anyOwner();

        assert cand.local();

        return readyLocal(cand);
    }

    /**
     * @param cand Local candidate added in any of the {@code addLocal(..)} methods.
     * @return Current lock owner.
     */
    @Nullable public GridCacheMvccCandidate readyLocal(GridCacheMvccCandidate cand) {
        assert cand.local();

        cand.setReady();

        reassign();

        return anyOwner();
    }

    /**
     * Marks near-local candidate as ready and makes locks reassignment. Following reorderings are performed when
     * candidate is marked ready:
     * <ul>
     *     <li/> All candidates preceding ready one are moved right after it.
     *     <li/> Near local candidate is assigned a mapped dht version. All remote non-pending candidates with
     *          version less then mapped dht version are marked as owned.
     * </ul>
     *
     * @param ver Version to mark as ready.
     * @param mappedVer Mapped dht version.
     * @param committedVers Committed versions.
     * @param rolledBackVers Rolled back versions.
     * @param pending Pending dht versions that are not owned and which version is less then mapped.
     * @return Lock owner after reassignment.
     */
    @Nullable public GridCacheMvccCandidate readyNearLocal(GridCacheVersion ver, GridCacheVersion mappedVer,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledBackVers,
        Collection<GridCacheVersion> pending) {
        GridCacheMvccCandidate cand = candidate(locs, ver);

        if (cand != null) {
            assert cand.nearLocal() : "Near local candidate is not marked as near local: " + cand;

            cand.setReady();

            boolean setMapped = cand.otherVersion(mappedVer);

            assert setMapped : "Failed to set mapped dht version for near local candidate [mappedVer=" +
                mappedVer + ", cand=" + cand + ']';

            // For near locals we move all not owned candidates after this one.
            List<GridCacheMvccCandidate> mvAfter = null;

            for (ListIterator<GridCacheMvccCandidate> it = locs.listIterator(); it.hasNext(); ) {
                GridCacheMvccCandidate c = it.next();

                assert c.nearLocal() : "Near local candidate is not marked as near local: " + c;

                if (c == cand) {
                    if (mvAfter != null)
                        for (GridCacheMvccCandidate mv : mvAfter)
                            it.add(mv);

                    break;
                }
                else {
                    if (c.owner())
                        continue;

                    assert !c.ready() :
                        "Cannot have more then one ready near-local candidate [c=" + c + ", cand=" + cand +
                            ", mvcc=" + this + ']';

                    it.remove();

                    if (mvAfter == null)
                        mvAfter = new LinkedList<>();

                    mvAfter.add(c);
                }
            }

            // Mark all remote candidates with less version as owner unless it is pending.
            if (rmts != null) {
                for (GridCacheMvccCandidate rmt : rmts) {
                    GridCacheVersion rmtVer = rmt.version();

                    if (rmtVer.isLess(mappedVer)) {
                        if (!pending.contains(rmtVer) &&
                            !mappedVer.equals(rmt.ownerVersion()))
                            rmt.setOwner();
                    }
                    else {
                        // Remote version is greater, so need to check if it was committed or rolled back.
                        if (committedVers.contains(rmtVer) || rolledBackVers.contains(rmtVer))
                            rmt.setOwner();
                    }
                }
            }

            reassign();
        }

        return anyOwner();
    }

    /**
     * Sets remote candidate to done.
     *
     * @param ver Version.
     * @param pending Pending versions.
     * @param committed Committed versions.
     * @param rolledback Rolledback versions.
     * @return Lock owner.
     */
    @Nullable public GridCacheMvccCandidate doneRemote(
        GridCacheVersion ver,
        Collection<GridCacheVersion> pending,
        Collection<GridCacheVersion> committed,
        Collection<GridCacheVersion> rolledback) {
        assert ver != null;

        if (log.isDebugEnabled())
            log.debug("Setting remote candidate to done [mvcc=" + this + ", ver=" + ver + "]");

        // Check remote candidate.
        GridCacheMvccCandidate cand = candidate(rmts, ver);

        if (cand != null) {
            assert rmts != null;
            assert !rmts.isEmpty();
            assert !cand.local() : "Remote candidate is marked as local: " + cand;
            assert !cand.nearLocal() : "Remote candidate is marked as near local: " + cand;

            cand.setOwner();
            cand.setUsed();

            List<GridCacheMvccCandidate> mvAfter = null;

            for (ListIterator<GridCacheMvccCandidate> it = rmts.listIterator(); it.hasNext(); ) {
                GridCacheMvccCandidate c = it.next();

                assert !c.nearLocal() : "Remote candidate marked as near local: " + c;

                if (c == cand) {
                    if (mvAfter != null)
                        for (GridCacheMvccCandidate mv : mvAfter)
                            it.add(mv);

                    break;
                }
                else if (!committed.contains(c.version()) && !rolledback.contains(c.version()) &&
                    pending.contains(c.version())) {
                    it.remove();

                    if (mvAfter == null)
                        mvAfter = new LinkedList<>();

                    mvAfter.add(c);
                }
            }
        }

        return anyOwner();
    }

    /**
     * For all remote candidates standing behind the candidate being salvaged marks their transactions
     * as system invalidate and marks these candidates as owned and used.
     *
     * @param ver Version to salvage.
     */
    public void salvageRemote(GridCacheVersion ver) {
        assert ver != null;

        GridCacheMvccCandidate cand = candidate(rmts, ver);

        if (cand != null) {
            assert rmts != null;
            assert !rmts.isEmpty();

            for (Iterator<GridCacheMvccCandidate> iter = rmts.iterator(); iter.hasNext(); ) {
                GridCacheMvccCandidate rmt = iter.next();

                // For salvaged candidate doneRemote will be called explicitly.
                if (rmt == cand)
                    break;

                // Only Near and DHT remote candidates should be released.
                assert !rmt.nearLocal();

                IgniteInternalTx tx = cctx.tm().tx(rmt.version());

                if (tx != null) {
                    tx.systemInvalidate(true);

                    rmt.setOwner();
                    rmt.setUsed();
                }
                else
                    iter.remove();
            }
        }
    }

    /**
     * Assigns local lock.
     */
    private void reassign() {
        GridCacheMvccCandidate firstRmt = null;

        if (rmts != null) {
            for (GridCacheMvccCandidate cand : rmts) {
                if (firstRmt == null)
                    firstRmt = cand;

                // If there is a remote owner, then local cannot be an owner,
                // so no reassignment happens.
                if (cand.owner())
                    return;
            }
        }

        if (locs != null) {
            for (ListIterator<GridCacheMvccCandidate> it = locs.listIterator(); it.hasNext(); ) {
                GridCacheMvccCandidate cand = it.next();

                if (cand.owner())
                    return;

                if (cand.ready()) {
                    GridCacheMvccCandidate prev = nonRollbackPrevious(cand);

                    // If previous has not been acquired, this candidate cannot acquire lock either,
                    // so we move on to the next one.
                    if (prev != null && !prev.owner())
                        continue;

                    boolean assigned = false;

                    if (!cctx.isNear() && firstRmt != null && cand.version().isGreater(firstRmt.version())) {
                        // Check previous candidates for 2 cases:
                        // 1. If this candidate is waiting for a smaller remote version,
                        //    then we must check if previous candidate is the owner and
                        //    has the same remote candidate version. In that case, we can
                        //    safely set this candidate to owner as well.
                        // 2. If this candidate is waiting for a smaller remote version,
                        //    then we must check if previous candidate is the owner and
                        //    any of the local candidates with versions smaller than first
                        //    remote version have the same key as the previous owner. In
                        //    that case, we can safely set this candidate to owner as well.
                        while (prev != null && prev.owner()) {
                            for (GridCacheMvccCandidate c : prev.parent().remoteMvccSnapshot()) {
                                if (c.version().equals(firstRmt.version())) {
                                    cand.setOwner();

                                    assigned = true;

                                    break; // For.
                                }
                            }

                            if (!assigned) {
                                for (GridCacheMvccCandidate c : locs) {
                                    if (c == cand || c.version().isGreater(firstRmt.version()))
                                        break;

                                    for (GridCacheMvccCandidate p = c.previous(); p != null; p = p.previous()) {
                                        if (p.key().equals(prev.key())) {
                                            cand.setOwner();

                                            assigned = true;

                                            break; // For.
                                        }
                                    }

                                    if (assigned)
                                        break; // For.
                                }
                            }

                            if (assigned)
                                break; // While.

                            prev = prev.previous();
                        }
                    }

                    if (!assigned) {
                        if (!cctx.isNear() && firstRmt != null) {
                            if (cand.version().isLess(firstRmt.version())) {
                                assert !cand.nearLocal();

                                cand.setOwner();

                                assigned = true;
                            }
                        }
                        else {
                            cand.setOwner();

                            assigned = true;
                        }
                    }

                    if (assigned) {
                        it.remove();

                        // Owner must be first in the list.
                        locs.addFirst(cand);
                    }

                    return;
                }
            }
        }
    }

    /**
     * @param cand Candidate to check.
     * @return First predecessor that is owner or is not used.
     */
    @Nullable private GridCacheMvccCandidate nonRollbackPrevious(GridCacheMvccCandidate cand) {
        for (GridCacheMvccCandidate c = cand.previous(); c != null; c = c.previous()) {
            if (c.owner() || !c.used())
                return c;
        }

        return null;
    }

    /**
     * Checks if lock should be assigned.
     *
     * @return Owner.
     */
    @Nullable public GridCacheMvccCandidate recheck() {
        reassign();

        return anyOwner();
    }

    /**
     * Local local release.
     * @return Removed lock candidate or <tt>null</tt> if candidate was not removed.
     */
    @Nullable public GridCacheMvccCandidate releaseLocal() {
        return releaseLocal(Thread.currentThread().getId());
    }

    /**
     * Local release.
     *
     * @param threadId ID of the thread.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate releaseLocal(long threadId) {
        GridCacheMvccCandidate owner = localOwner();

        if (owner == null || owner.threadId() != threadId)
            // Release had no effect.
            return owner;

        owner.setUsed();

        remove0(owner.version(), true);

        return anyOwner();
    }

    /**
     * Removes lock even if it is not owner.
     *
     * @param ver Lock version.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate remove(GridCacheVersion ver) {
        remove0(ver, false);

        return anyOwner();
    }

    /**
     * Removes all candidates for node.
     *
     * @param nodeId Node ID.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate removeExplicitNodeCandidates(UUID nodeId) {
        if (rmts != null) {
            for (Iterator<GridCacheMvccCandidate> it = rmts.iterator(); it.hasNext(); ) {
                GridCacheMvccCandidate cand = it.next();

                if (!cand.tx() && (nodeId.equals(cand.nodeId()) || nodeId.equals(cand.otherNodeId()))) {
                    cand.setUsed(); // Mark as used to be consistent.
                    cand.setRemoved();

                    it.remove();
                }
            }

            if (rmts.isEmpty())
                rmts = null;
        }

        if (locs != null) {
            for (Iterator<GridCacheMvccCandidate> it = locs.iterator(); it.hasNext(); ) {
                GridCacheMvccCandidate cand = it.next();

                if (!cand.tx() && nodeId.equals(cand.otherNodeId()) && cand.dhtLocal()) {
                    cand.setUsed(); // Mark as used to be consistent.
                    cand.setRemoved();

                    it.remove();
                }
            }

            if (locs.isEmpty())
                locs = null;
        }

        reassign();

        return anyOwner();
    }

    /**
     * Gets candidate for lock ID.
     *
     * @param ver Lock version.
     * @return Candidate or <tt>null</tt> if there is no candidate for given ID.
     */
    @Nullable public GridCacheMvccCandidate candidate(GridCacheVersion ver) {
        GridCacheMvccCandidate cand = candidate(locs, ver);

        if (cand == null)
            cand = candidate(rmts, ver);

        return cand;
    }

    /**
     * Gets candidate for lock ID.
     *
     * @param threadId Thread ID.
     * @return Candidate or <tt>null</tt> if there is no candidate for given ID.
     */
    @Nullable public GridCacheMvccCandidate localCandidate(long threadId) {
        // Don't return reentries.
        return localCandidate(threadId, false);
    }

    /**
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @return Remote candidate.
     */
    @Nullable public GridCacheMvccCandidate remoteCandidate(UUID nodeId, long threadId) {
        if (rmts != null)
            for (GridCacheMvccCandidate c : rmts)
                if (c.nodeId().equals(nodeId) && c.threadId() == threadId)
                    return c;

        return null;
    }

    /**
     * Near local candidate.
     *
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @return Remote candidate.
     */
    @Nullable public GridCacheMvccCandidate localCandidate(UUID nodeId, long threadId) {
        if (locs != null)
            for (GridCacheMvccCandidate c : locs)
                if (c.nodeId().equals(nodeId) && c.threadId() == threadId)
                    return c;

        return null;
    }

    /**
     *
     * @param ver Version.
     * @return {@code True} if candidate with given version exists.
     */
    public boolean hasCandidate(GridCacheVersion ver) {
        return candidate(ver) != null;
    }

    /**
     * @param reentry Reentry flag.
     * @return Collection of local candidates.
     */
    public List<GridCacheMvccCandidate> localCandidatesNoCopy(boolean reentry) {
        return candidates(locs, reentry, false, cctx.emptyVersion());
    }

    /**
     * @param excludeVers Exclude versions.
     * @return Collection of local candidates.
     */
    public Collection<GridCacheMvccCandidate> localCandidates(GridCacheVersion... excludeVers) {
        return candidates(locs, false, true, excludeVers);
    }

    /**
     * @param reentries Flag to include reentries.
     * @param excludeVers Exclude versions.
     * @return Collection of local candidates.
     */
    public List<GridCacheMvccCandidate> localCandidates(boolean reentries,
        GridCacheVersion... excludeVers) {
        return candidates(locs, reentries, true, excludeVers);
    }

    /**
     * @param excludeVers Exclude versions.
     * @return Collection of remote candidates.
     */
    public List<GridCacheMvccCandidate> remoteCandidates(GridCacheVersion... excludeVers) {
        return candidates(rmts, false, true, excludeVers);
    }

    /**
     * @param col Collection of candidates.
     * @param reentries Reentry flag.
     * @param cp Whether to copy or not.
     * @param excludeVers Exclude versions.
     * @return Collection of candidates minus the exclude versions.
     */
    private List<GridCacheMvccCandidate> candidates(List<GridCacheMvccCandidate> col,
        boolean reentries, boolean cp, GridCacheVersion... excludeVers) {
        if (col == null)
            return Collections.emptyList();

        assert !col.isEmpty();

        if (!cp && F.isEmpty(excludeVers))
            return col;

        List<GridCacheMvccCandidate> cands = new ArrayList<>(col.size());

        for (GridCacheMvccCandidate c : col) {
            // Don't include reentries.
            if ((!c.reentry() || (reentries && c.reentry())) && !U.containsObjectArray(excludeVers, c.version()))
                cands.add(c);
        }

        return cands;
    }

    /**
     * @return {@code True} if lock is owner by current thread.
     */
    public boolean isLocallyOwnedByCurrentThread() {
        return isLocallyOwnedByThread(Thread.currentThread().getId(), true);
    }

    /**
     * @param threadId Thread ID to check.
     * @param exclude Versions to ignore.
     * @return {@code True} if lock is owned by the thread with given ID.
     */
    public boolean isLocallyOwnedByThread(long threadId, boolean allowDhtLoc, GridCacheVersion... exclude) {
        GridCacheMvccCandidate owner = localOwner();

        return owner != null && owner.threadId() == threadId && owner.nodeId().equals(cctx.nodeId()) &&
            (allowDhtLoc || !owner.dhtLocal()) && !U.containsObjectArray(exclude, owner.version());
    }

    /**
     * @param threadId Thread ID.
     * @param nodeId Node ID.
     * @return {@code True} if lock is held by given thread and node IDs.
     */
    public boolean isLockedByThread(long threadId, UUID nodeId) {
        GridCacheMvccCandidate owner = anyOwner();

        return owner != null && owner.threadId() == threadId && owner.nodeId().equals(nodeId);
    }

    /**
     * @return {@code True} if lock is owned by any thread or node.
     */
    public boolean isOwnedByAny() {
        return anyOwner() != null;
    }

    /**
     *
     * @param lockVer ID of lock candidate.
     * @return {@code True} if candidate is owner.
     */
    public boolean isLocallyOwned(GridCacheVersion lockVer) {
        GridCacheMvccCandidate owner = localOwner();

        return owner != null && owner.version().equals(lockVer);
    }

    /**
     * @param lockVer Lock ID.
     * @param threadId Thread ID.
     * @return {@code True} if locked by lock ID or thread ID.
     */
    public boolean isLocallyOwnedByIdOrThread(GridCacheVersion lockVer, long threadId) {
        GridCacheMvccCandidate owner = localOwner();

        return owner != null && (owner.version().equals(lockVer) || owner.threadId() == threadId);
    }

    /**
     * @return First remote entry or <tt>null</tt>.
     */
    @Nullable public GridCacheMvccCandidate firstRemote() {
        return rmts == null ? null : rmts.getFirst();
    }

    /**
     * @return First local entry or <tt>null</tt>.
     */
    @Nullable public GridCacheMvccCandidate firstLocal() {
        return locs == null ? null : locs.getFirst();
    }

    /**
     * @param ver Version to check for ownership.
     * @return {@code True} if lock is owned by the specified version.
     */
    public boolean isOwnedBy(GridCacheVersion ver) {
        GridCacheMvccCandidate cand = anyOwner();

        return cand != null && cand.version().equals(ver);
    }

    /** {@inheritDoc} */
    @Override public String toString() { // Synchronize to ensure one-thread at a time.
        return S.toString(GridCacheMvcc.class, this);
    }
}