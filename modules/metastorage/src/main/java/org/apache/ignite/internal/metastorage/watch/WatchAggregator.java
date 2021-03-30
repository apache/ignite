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

package org.apache.ignite.internal.metastorage.watch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.metastorage.client.EntryEvent;
import org.apache.ignite.metastorage.client.WatchEvent;
import org.apache.ignite.metastorage.client.WatchListener;
import org.jetbrains.annotations.NotNull;

/**
 * Needed to aggregate multiple watches to one aggregated watch.
 * This approach needed to provide the following additional guarantees to watching mechanism:
 * - watch events will be processed sequentially
 * - watch events will be resolved in the order of watch registration
 */
public class WatchAggregator {
    /**
     * Watches' map must be synchronized because of changes from WatchListener in separate thread.
     */
    private final Map<Long, Watch> watches = Collections.synchronizedMap(new LinkedHashMap<>());

    /** Simple auto increment id for internal watches. */
    private final AtomicLong idCntr = new AtomicLong(0);

    /**
     * Adds new watch with simple exact criterion.
     *
     * @param key Key for watching.
     * @param lsnr Listener which will be executed on watch event.
     * @return id of registered watch. Can be used for remove watch from later.
     */
    public long add(ByteArray key, WatchListener lsnr) {
        var watch = new Watch(new KeyCriterion.ExactCriterion(key), lsnr);
        var id = idCntr.incrementAndGet();
        watches.put(id, watch);
        return id;
    }

    /**
     * Adds new watch with filter by key prefix.
     *
     * @param key Prefix for key.
     * @param lsnr Listener which will be executed on watch event.
     * @return id of registered watch. Can be used for remove watch from later.
     */
    public long addPrefix(ByteArray key, WatchListener lsnr) {
        var watch = new Watch(new KeyCriterion.PrefixCriterion(key), lsnr);
        var id = idCntr.incrementAndGet();
        watches.put(id, watch);
        return id;
    }

    /**
     * Adds new watch with filter by collection of keys.
     *
     * @param keys Collection of keys to listen.
     * @param lsnr Listener which will be executed on watch event.
     * @return id of registered watch. Can be used for remove watch from later.
     */
    public long add(Collection<ByteArray> keys, WatchListener lsnr) {
        var watch = new Watch(new KeyCriterion.CollectionCriterion(keys), lsnr);
        var id = idCntr.incrementAndGet();
        watches.put(id, watch);
        return id;
    }

    /**
     * Adds new watch with filter by collection of keys.
     *
     * @param from Start key of range to listen.
     * @param to End key of range (exclusively)..
     * @param lsnr Listener which will be executed on watch event.
     * @return id of registered watch. Can be used for remove watch from later.
     */
    public long add(ByteArray from, ByteArray to, WatchListener lsnr) {
        var watch = new Watch(new KeyCriterion.RangeCriterion(from, to), lsnr);
        var id = idCntr.incrementAndGet();
        watches.put(id, watch);
        return id;
    }

    /**
     * Cancel watch by id.
     *
     * @param id of watch to cancel.
     */
    public void cancel(long id) {
        watches.remove(id);
    }

    /**
     * Cancel multiple watches by ids.
     *
     * @param ids of watches to cancel.
     */
    public void cancelAll(Collection<Long> ids) {
        watches.keySet().removeAll(ids);
    }

    /**
     * Produce watch with aggregated key criterion and general watch listener dispatcher.
     *
     * @param revision start revision to listen event.
     * @param saveRevisionAct action to commit keys-revision pair to persistent store for processed keys.
     * @return result aggregated watch.
     */
    public Optional<AggregatedWatch> watch(
            long revision,
            BiConsumer<Collection<IgniteBiTuple<ByteArray, byte[]>>, Long> saveRevisionAct
    ) {
        synchronized (watches) {
            if (watches.isEmpty())
                return Optional.empty();
            else
                return Optional.of(new AggregatedWatch(inferGeneralCriteria(), revision, watchListener(saveRevisionAct)));
        }
    }

    /**
     * Returns general criterion, which overlays all aggregated criteria.
     *
     * @return aggregated criterion.
     */
    // TODO: IGNITE-14667 We can do it better than infer range always
    private KeyCriterion inferGeneralCriteria() {
        return new KeyCriterion.RangeCriterion(
            watches.values().stream().map(w -> w.keyCriterion().toRange().getKey()).min(ByteArray::compareTo).get(),
            watches.values().stream().map(w -> w.keyCriterion().toRange().getValue()).max(ByteArray::compareTo).get()
        );
    }

    /**
     * Produces the watch listener, which will dispatch events to appropriate watches.
     *
     * @param storeRevision action to commit keys-revision pair to persistent store for processed keys.
     * @return watch listener, which will dispatch events to appropriate watches.
     */
    private WatchListener watchListener(BiConsumer<Collection<IgniteBiTuple<ByteArray, byte[]>>, Long> storeRevision) {
        // Copy watches to separate collection, because all changes on the WatchAggregator watches
        // shouldn't be propagated to listener watches immediately.
        // WatchAggregator will be redeployed with new watches if needed instead.
        final LinkedHashMap<Long, Watch> cpWatches = new LinkedHashMap<>(watches);

        return new WatchListener() {

            @Override public boolean onUpdate(@NotNull WatchEvent evt) {
                var watchIt = cpWatches.entrySet().iterator();
                Collection<Long> toCancel = new ArrayList<>();

                while (watchIt.hasNext()) {
                    Map.Entry<Long, WatchAggregator.Watch> entry = watchIt.next();
                    WatchAggregator.Watch watch = entry.getValue();
                    var filteredEvts = new ArrayList<EntryEvent>();

                    for (EntryEvent entryEvt : evt.entryEvents()) {
                        if (watch.keyCriterion().contains(entryEvt.oldEntry().key()))
                            filteredEvts.add(entryEvt);
                    }

                    if (!filteredEvts.isEmpty()) {
                        if (!watch.listener().onUpdate(new WatchEvent(filteredEvts))) {
                            watchIt.remove();

                            toCancel.add(entry.getKey());
                        }
                    }
                }

                // Cancel finished watches from the global watch map
                // to prevent finished watches from redeploy.
                if (!toCancel.isEmpty())
                    cancelAll(toCancel);

                var revision = 0L;
                var entries = new ArrayList<IgniteBiTuple<ByteArray, byte[]>>();
                for (EntryEvent entryEvt: evt.entryEvents()) {
                    revision = entryEvt.newEntry().revision();

                    entries.add(new IgniteBiTuple<>(entryEvt.newEntry().key(), entryEvt.newEntry().value()));
                }

                storeRevision.accept(entries, revision);

                return true;
            }

            @Override public void onError(@NotNull Throwable e) {
                watches.values().forEach(w -> w.listener().onError(e));
            }
        };
    }

    /**
     * (key criterion, watch listener) container.
     */
    private static class Watch {
        /** Key criterion. */
        private final KeyCriterion keyCriterion;

        /** Watch listener. */
        private final WatchListener lsnr;

        /** Creates the watch. */
        private Watch(KeyCriterion keyCriterion, WatchListener lsnr) {
            this.keyCriterion = keyCriterion;
            this.lsnr = lsnr;
        }

        /**
         * @return key criterion.
         */
        public KeyCriterion keyCriterion() {
            return keyCriterion;
        }

        /**
         * @return watch listener.
         */
        public WatchListener listener() {
            return lsnr;
        }
    }
}
