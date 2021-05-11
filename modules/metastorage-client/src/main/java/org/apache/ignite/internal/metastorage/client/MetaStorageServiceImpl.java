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

package org.apache.ignite.internal.metastorage.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.metastorage.common.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.PutCommand;
import org.apache.ignite.internal.metastorage.common.command.RangeCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.WatchExactKeysCommand;
import org.apache.ignite.internal.metastorage.common.command.WatchRangeKeysCommand;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Condition;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetaStorageService} implementation.
 */
public class MetaStorageServiceImpl implements MetaStorageService {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageServiceImpl.class);

    /** Meta storage raft group service. */
    private final RaftGroupService metaStorageRaftGrpSvc;

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.
    /** Watch processor, that uses pulling logic in order to retrieve watch notifications from server. */
    private final WatchProcessor watchProcessor;

    /**
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     */
    public MetaStorageServiceImpl(RaftGroupService metaStorageRaftGrpSvc) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.watchProcessor = new WatchProcessor();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> get(@NotNull Key key) {
        return metaStorageRaftGrpSvc.run(new GetCommand(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> get(@NotNull Key key, long revUpperBound) {
        return metaStorageRaftGrpSvc.run(new GetCommand(key, revUpperBound));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys) {
        return metaStorageRaftGrpSvc.<Collection<Entry>>run(new GetAllCommand(keys)).
            thenApply(entries -> entries.stream().collect(Collectors.toMap(Entry::key, Function.identity())));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys, long revUpperBound) {
        return metaStorageRaftGrpSvc.<Collection<Entry>>run(new GetAllCommand(keys, revUpperBound)).
            thenApply(entries -> entries.stream().collect(Collectors.toMap(Entry::key, Function.identity())));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> put(@NotNull Key key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.run(new PutCommand(key, value));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> getAndPut(@NotNull Key key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.run(new GetAndPutCommand(key, value));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAll(@NotNull Map<Key, byte[]> vals) {
        return metaStorageRaftGrpSvc.run(new PutAllCommand(vals));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAndPutAll(@NotNull Map<Key, byte[]> vals) {
        List<Key> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();

        vals.forEach((key, value) -> {
            keys.add(key);
            values.add(value);
        });

        return metaStorageRaftGrpSvc.<Collection<Entry>>run(new GetAndPutAllCommand(keys, values)).
            thenApply(entries -> entries.stream().collect(Collectors.toMap(Entry::key, Function.identity())));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> remove(@NotNull Key key) {
        return metaStorageRaftGrpSvc.run(new RemoveCommand(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull Key key) {
        return metaStorageRaftGrpSvc.run(new GetAndRemoveCommand(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> removeAll(@NotNull Collection<Key> keys) {
        return metaStorageRaftGrpSvc.run(new RemoveAllCommand(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAndRemoveAll(@NotNull Collection<Key> keys) {
        return metaStorageRaftGrpSvc.<Collection<Entry>>run(new GetAndRemoveAllCommand(keys)).
            thenApply(entries -> entries.stream().collect(Collectors.toMap(Entry::key, Function.identity())));
    }

    // TODO: IGNITE-14389 Implement.
    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> invoke(@NotNull Condition condition,
        @NotNull Collection<Operation> success, @NotNull Collection<Operation> failure) {
        return null;
    }

    // TODO: IGNITE-14389 Either implement or remove this method.
    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> getAndInvoke(@NotNull Key key, @NotNull Condition condition,
        @NotNull Operation success, @NotNull Operation failure) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo, long revUpperBound) {
        return new CursorImpl<>(
            metaStorageRaftGrpSvc,
            metaStorageRaftGrpSvc.run(new RangeCommand(keyFrom, keyTo, revUpperBound))
        );
    }

    /** {@inheritDoc} */
    @Override public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo) {
        return new CursorImpl<>(
            metaStorageRaftGrpSvc,
            metaStorageRaftGrpSvc.run(new RangeCommand(keyFrom, keyTo))
        );
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<IgniteUuid> watch(
        @Nullable Key keyFrom,
        @Nullable Key keyTo,
        long revision,
        @NotNull WatchListener lsnr
    ) {
        CompletableFuture<IgniteUuid> watchRes =
            metaStorageRaftGrpSvc.run(new WatchRangeKeysCommand(keyFrom, keyTo, revision));

        watchRes.thenAccept(
            watchId -> watchProcessor.addWatch(
                watchId,
                new CursorImpl<>(metaStorageRaftGrpSvc, watchRes),
                lsnr
            )
        );

        return watchRes;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<IgniteUuid> watch(
        @NotNull Key key,
        long revision,
        @NotNull WatchListener lsnr
    ) {
        return watch(key, null, revision, lsnr);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<IgniteUuid> watch(
        @NotNull Collection<Key> keys,
        long revision,
        @NotNull WatchListener lsnr
    ) {
        CompletableFuture<IgniteUuid> watchRes =
            metaStorageRaftGrpSvc.run(new WatchExactKeysCommand(keys, revision));

        watchRes.thenAccept(
            watchId -> watchProcessor.addWatch(
                watchId,
                new CursorImpl<>(metaStorageRaftGrpSvc, watchRes),
                lsnr
            )
        );

        return watchRes;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        return CompletableFuture.runAsync(() -> watchProcessor.stopWatch(id));
    }

    // TODO: IGNITE-14389 Implement.
    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> compact() {
        return null;
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.
    /** Watch processor, that manages {@link Watcher} threads. */
    private final class WatchProcessor {
        /** Active Watcher threads that process notification pulling logic. */
        private final Map<IgniteUuid, Watcher> watchers = new ConcurrentHashMap<>();

        /**
         * Starts exclusive thread per watch that implement watch pulling logic and
         * calls {@link WatchListener#onUpdate(Iterable)}} or {@link WatchListener#onError(Throwable)}.
         *
         * @param watchId Watch id.
         * @param cursor Watch Cursor.
         * @param lsnr The listener which receives and handles watch updates.
         */
        private void addWatch(IgniteUuid watchId, CursorImpl<WatchEvent> cursor, WatchListener lsnr) {
            Watcher watcher = new Watcher(cursor, lsnr);

            watchers.put(watchId, watcher);

            watcher.start();
        }

        /**
         * Closes server cursor and interrupts watch pulling thread.
         *
         * @param watchId Watch id.
         */
        private void stopWatch(IgniteUuid watchId) {
            watchers.computeIfPresent(
                watchId,
                (k, v) -> {
                    CompletableFuture.runAsync(v::interrupt).thenRun(() -> {
                        try {
                            v.cursor.close();
                        }
                        catch (InterruptedException e) {
                            throw new IgniteInternalException(e);
                        }
                        catch (Exception e) {
                            // TODO: IGNITE-14693 Implement MetaStorage exception handling logic.
                            LOG.error("Unexpected exception", e);
                        }
                    });
                    return null;
                }
            );
        }

        /** Watcher thread, uses pulling logic in order to retrieve watch notifications from server */
        private final class Watcher extends Thread {
            /** Watch event cursor. */
            private Cursor<WatchEvent> cursor;

            /** The listener which receives and handles watch updates. */
            private WatchListener lsnr;

            /**
             * @param cursor Watch event cursor.
             * @param lsnr The listener which receives and handles watch updates.
             */
            Watcher(Cursor<WatchEvent> cursor, WatchListener lsnr) {
                this.cursor = cursor;
                this.lsnr = lsnr;
            }

            /**
             * Pulls watch events from server side with the help of cursor.iterator.hasNext()/next()
             * in the while(true) loop. Collects watch events with same revision and fires either onUpdate or onError().
             */
            @Override public void run() {
                long rev = -1;

                List<WatchEvent> sameRevisionEvts = new ArrayList<>();

                Iterator<WatchEvent> watchEvtsIter = cursor.iterator();

                while (true) {
                    try {
                        if (watchEvtsIter.hasNext()) {
                            WatchEvent watchEvt = null;

                            try {
                                watchEvt = watchEvtsIter.next();
                            }
                            catch (Throwable e) {
                                lsnr.onError(e);
                            }

                            assert watchEvt != null;

                            if (watchEvt.newEntry().revision() == rev)
                                sameRevisionEvts.add(watchEvt);
                            else {
                                rev = watchEvt.newEntry().revision();

                                if (!sameRevisionEvts.isEmpty()) {
                                    lsnr.onUpdate(sameRevisionEvts);

                                    sameRevisionEvts.clear();
                                }

                                sameRevisionEvts.add(watchEvt);
                            }
                        }
                        else
                            Thread.sleep(10);
                    }
                    catch (Throwable e) {
                        if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException)
                            break;
                        else {
                            // TODO: IGNITE-14693 Implement MetaStorage exception handling logic.
                            LOG.error("Unexpected exception", e);
                        }
                    }
                }
            }
        }
    }
}
