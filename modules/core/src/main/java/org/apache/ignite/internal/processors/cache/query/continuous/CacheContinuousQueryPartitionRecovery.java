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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.cache.event.CacheEntryEvent;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class CacheContinuousQueryPartitionRecovery {
    /** Event which means hole in sequence. */
    private static final CacheContinuousQueryEntry HOLE;

    static {
        HOLE = new CacheContinuousQueryEntry();

        HOLE.markFiltered();
    }

    /** */
    private static final int MAX_BUFF_SIZE = CacheContinuousQueryHandler.LSNR_MAX_BUF_SIZE;

    /** */
    private IgniteLogger log;

    /** */
    private long lastFiredEvt;

    /** */
    private AffinityTopologyVersion curTop = AffinityTopologyVersion.NONE;

    /** */
    private final TreeMap<Long, CacheContinuousQueryEntry> pendingEvts = new TreeMap<>();

    /**
     * @param log Logger.
     * @param topVer Topology version.
     * @param initCntr Update counters.
     */
    CacheContinuousQueryPartitionRecovery(IgniteLogger log, AffinityTopologyVersion topVer, @Nullable Long initCntr) {
        this.log = log;

        if (initCntr != null) {
            assert topVer.topologyVersion() > 0 : topVer;

            this.lastFiredEvt = initCntr;

            curTop = topVer;
        }
    }

    /**
     * Resets cached topology.
     */
    void resetTopologyCache() {
        curTop = AffinityTopologyVersion.NONE;
    }

    /**
     * Add continuous entry.
     *
     * @param cctx Cache context.
     * @param cache Cache.
     * @param entry Cache continuous query entry.
     * @return Collection entries which will be fired. This collection should contains only non-filtered events.
     */
    <K, V> Collection<CacheEntryEvent<? extends K, ? extends V>> collectEntries(
        CacheContinuousQueryEntry entry,
        GridCacheContext cctx,
        IgniteCache cache
    ) {
        assert entry != null;

        if (entry.topologyVersion() == null) { // Possible if entry is sent from old node.
            assert entry.updateCounter() == 0L : entry;

            return F.<CacheEntryEvent<? extends K, ? extends V>>
                asList(new CacheContinuousQueryEvent<K, V>(cache, cctx, entry));
        }

        List<CacheEntryEvent<? extends K, ? extends V>> entries;

        synchronized (pendingEvts) {
            if (log.isDebugEnabled()) {
                log.debug("Handling event [lastFiredEvt=" + lastFiredEvt +
                    ", curTop=" + curTop +
                    ", entUpdCnt=" + entry.updateCounter() +
                    ", partId=" + entry.partition() +
                    ", pendingEvts=" + pendingEvts + ']');
            }

            // Received first event.
            if (curTop == AffinityTopologyVersion.NONE) {
                lastFiredEvt = entry.updateCounter();

                curTop = entry.topologyVersion();

                if (log.isDebugEnabled()) {
                    log.debug("First event [lastFiredEvt=" + lastFiredEvt +
                        ", curTop=" + curTop +
                        ", entUpdCnt=" + entry.updateCounter() +
                        ", partId=" + entry.partition() + ']');
                }

                return !entry.isFiltered() ?
                    F.<CacheEntryEvent<? extends K, ? extends V>>
                        asList(new CacheContinuousQueryEvent<K, V>(cache, cctx, entry)) :
                    Collections.<CacheEntryEvent<? extends K, ? extends V>>emptyList();
            }

            if (curTop.compareTo(entry.topologyVersion()) < 0) {
                if (entry.updateCounter() == 1L && !entry.isBackup()) {
                    entries = new ArrayList<>(pendingEvts.size());

                    for (CacheContinuousQueryEntry evt : pendingEvts.values()) {
                        if (evt != HOLE && !evt.isFiltered())
                            entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, evt));
                    }

                    pendingEvts.clear();

                    curTop = entry.topologyVersion();

                    lastFiredEvt = entry.updateCounter();

                    if (!entry.isFiltered())
                        entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, entry));

                    if (log.isDebugEnabled())
                        log.debug("Partition was lost [lastFiredEvt=" + lastFiredEvt +
                            ", curTop=" + curTop +
                            ", entUpdCnt=" + entry.updateCounter() +
                            ", partId=" + entry.partition() +
                            ", pendingEvts=" + pendingEvts + ']');

                    return entries;
                }

                curTop = entry.topologyVersion();
            }

            // Check duplicate.
            if (entry.updateCounter() > lastFiredEvt)
                pendingEvts.put(entry.updateCounter(), entry);
            else {
                if (log.isDebugEnabled())
                    log.debug("Skip duplicate continuous query message: " + entry);

                return Collections.emptyList();
            }

            if (pendingEvts.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Nothing sent to listener [lastFiredEvt=" + lastFiredEvt +
                        ", curTop=" + curTop +
                        ", entUpdCnt=" + entry.updateCounter() +
                        ", partId=" + entry.partition() + ']');
                }

                return Collections.emptyList();
            }

            Iterator<Map.Entry<Long, CacheContinuousQueryEntry>> iter = pendingEvts.entrySet().iterator();

            entries = new ArrayList<>();

            if (pendingEvts.size() >= MAX_BUFF_SIZE) {
                if (log.isDebugEnabled()) {
                    log.debug("Pending events reached max of buffer size [lastFiredEvt=" + lastFiredEvt +
                        ", curTop=" + curTop +
                        ", entUpdCnt=" + entry.updateCounter() +
                        ", partId=" + entry.partition() +
                        ", pendingEvts=" + pendingEvts + ']');
                }

                LT.warn(log, "Pending events reached max of buffer size [cache=" + cctx.name() +
                    ", bufSize=" + MAX_BUFF_SIZE +
                    ", partId=" + entry.partition() + ']');

                for (int i = 0; i < MAX_BUFF_SIZE - (MAX_BUFF_SIZE / 10); i++) {
                    Map.Entry<Long, CacheContinuousQueryEntry> e = iter.next();

                    if (e.getValue() != HOLE && !e.getValue().isFiltered())
                        entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, e.getValue()));

                    lastFiredEvt = e.getKey();

                    iter.remove();
                }
            }
            else {
                boolean skippedFiltered = false;

                while (iter.hasNext()) {
                    Map.Entry<Long, CacheContinuousQueryEntry> e = iter.next();

                    CacheContinuousQueryEntry pending = e.getValue();

                    long filtered = pending.filteredCount();

                    boolean fire = e.getKey() == lastFiredEvt + 1;;

                    if (!fire && filtered > 0)
                        fire = e.getKey() - filtered <= lastFiredEvt + 1;

                    if (fire) {
                        lastFiredEvt = e.getKey();

                        if (e.getValue() != HOLE && !e.getValue().isFiltered())
                            entries.add(new CacheContinuousQueryEvent<K, V>(cache, cctx, pending));

                        iter.remove();
                    }
                    else {
                        if (pending.isFiltered())
                            skippedFiltered = true;
                        else
                            break;
                    }
                }

                if (skippedFiltered)
                    pendingEvts.headMap(lastFiredEvt).clear();
            }

            if (log.isDebugEnabled()) {
                log.debug("Will send to listener the following events [entries=" + entries +
                    ", lastFiredEvt=" + lastFiredEvt +
                    ", curTop=" + curTop +
                    ", entUpdCnt=" + entry.updateCounter() +
                    ", partId=" + entry.partition() +
                    ", pendingEvts=" + pendingEvts + ']');
            }
        }

        return entries;
    }
}
