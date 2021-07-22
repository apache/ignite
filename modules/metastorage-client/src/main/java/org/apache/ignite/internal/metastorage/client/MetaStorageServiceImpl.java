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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.metastorage.common.command.ConditionInfo;
import org.apache.ignite.internal.metastorage.common.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.common.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.common.command.OperationInfo;
import org.apache.ignite.internal.metastorage.common.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.PutCommand;
import org.apache.ignite.internal.metastorage.common.command.RangeCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.common.command.WatchExactKeysCommand;
import org.apache.ignite.internal.metastorage.common.command.WatchRangeKeysCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorsCloseCommand;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
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

    /** Local node id. */
    private final String localNodeId;

    /**
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param localNodeId Local node id.
     */
    public MetaStorageServiceImpl(RaftGroupService metaStorageRaftGrpSvc, String localNodeId) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.watchProcessor = new WatchProcessor();
        this.localNodeId = localNodeId;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
        return metaStorageRaftGrpSvc.run(new GetCommand(key)).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
        return metaStorageRaftGrpSvc.run(new GetCommand(key, revUpperBound))
                .thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        return metaStorageRaftGrpSvc.run(new GetAllCommand(keys))
                .thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        return metaStorageRaftGrpSvc.run(new GetAllCommand(keys, revUpperBound)).
                thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.run(new PutCommand(key, value));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.run(new GetAndPutCommand(key, value))
                .thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        return metaStorageRaftGrpSvc.run(new PutAllCommand(vals));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
        return metaStorageRaftGrpSvc.run(new GetAndPutAllCommand(vals)).
            thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        return metaStorageRaftGrpSvc.run(new RemoveCommand(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
        return metaStorageRaftGrpSvc.run(new GetAndRemoveCommand(key))
                .thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys) {
        return metaStorageRaftGrpSvc.run(new RemoveAllCommand(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys) {
        return metaStorageRaftGrpSvc.run(new GetAndRemoveAllCommand(keys)).
            thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition condition,
            @NotNull Operation success,
            @NotNull Operation failure
    ) {
        return invoke(condition, List.of(success), List.of(failure));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition condition,
            @NotNull Collection<Operation> success,
            @NotNull Collection<Operation> failure
    ) {
        ConditionInfo cond = toConditionInfo(condition);

        List<OperationInfo> successOps = toOperationInfos(success);

        List<OperationInfo> failureOps = toOperationInfos(failure);

        return metaStorageRaftGrpSvc.run(new InvokeCommand(cond, successOps, failureOps));
    }

    /** {@inheritDoc} */
    @Override public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        return new CursorImpl<>(
                metaStorageRaftGrpSvc,
                metaStorageRaftGrpSvc.run(new RangeCommand(keyFrom, keyTo, revUpperBound, localNodeId)),
                MetaStorageServiceImpl::singleEntryResult
        );
    }

    /** {@inheritDoc} */
    @Override public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return new CursorImpl<>(
                metaStorageRaftGrpSvc,
                metaStorageRaftGrpSvc.run(new RangeCommand(keyFrom, keyTo, localNodeId)),
                MetaStorageServiceImpl::singleEntryResult
        );
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<IgniteUuid> watch(
        @Nullable ByteArray keyFrom,
        @Nullable ByteArray keyTo,
        long revision,
        @NotNull WatchListener lsnr
    ) {
        CompletableFuture<IgniteUuid> watchRes =
            metaStorageRaftGrpSvc.run(new WatchRangeKeysCommand(keyFrom, keyTo, revision, localNodeId));

        watchRes.thenAccept(
            watchId -> watchProcessor.addWatch(
                watchId,
                new CursorImpl<>(metaStorageRaftGrpSvc, watchRes, MetaStorageServiceImpl::watchResponse),
                lsnr
            )
        );

        return watchRes;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<IgniteUuid> watch(
        @NotNull ByteArray key,
        long revision,
        @NotNull WatchListener lsnr
    ) {
        return watch(key, null, revision, lsnr);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<IgniteUuid> watch(
        @NotNull Set<ByteArray> keys,
        long revision,
        @NotNull WatchListener lsnr
    ) {
        CompletableFuture<IgniteUuid> watchRes =
            metaStorageRaftGrpSvc.run(new WatchExactKeysCommand(keys, revision, localNodeId));

        watchRes.thenAccept(
            watchId -> watchProcessor.addWatch(
                watchId,
                new CursorImpl<>(metaStorageRaftGrpSvc, watchRes, MetaStorageServiceImpl::watchResponse),
                lsnr
            )
        );

        return watchRes;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        return CompletableFuture.runAsync(() -> watchProcessor.stopWatch(id));
    }

    // TODO: IGNITE-14734 Implement.
    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> compact() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> closeCursors(@NotNull String nodeId) {
        return metaStorageRaftGrpSvc.run(new CursorsCloseCommand(nodeId));
    }

    /** */
    private static List<OperationInfo> toOperationInfos(Collection<Operation> ops) {
        List<OperationInfo> res = new ArrayList<>(ops.size());

        for (Operation op : ops) {
            OperationInfo info = null;

            if (op.type() == OperationType.NO_OP)
                info = new OperationInfo(null, null, OperationType.NO_OP);
            else if (op.type() == OperationType.REMOVE)
                info = new OperationInfo(((Operation.RemoveOp)op.inner()).key(), null, OperationType.REMOVE);
            else if (op.type() == OperationType.PUT) {
                Operation.PutOp inner = (Operation.PutOp)op.inner();

                info = new OperationInfo(inner.key(), inner.value(), OperationType.PUT);
            }
            else
                assert false : "Unknown operation type " + op.type();

            res.add(info);
        }

        return res;
    }

    /** */
    private static ConditionInfo toConditionInfo(@NotNull Condition condition) {
        ConditionInfo cnd = null;

        Object obj = condition.inner();

        if (obj instanceof Condition.ExistenceCondition) {
            Condition.ExistenceCondition inner = (Condition.ExistenceCondition)obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), null, 0);
        }
        else if (obj instanceof Condition.TombstoneCondition) {
            Condition.TombstoneCondition inner = (Condition.TombstoneCondition)obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), null, 0);
        }
        else if (obj instanceof Condition.RevisionCondition) {
            Condition.RevisionCondition inner = (Condition.RevisionCondition)obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), null, inner.revision());
        }
        else if (obj instanceof Condition.ValueCondition) {
            Condition.ValueCondition inner = (Condition.ValueCondition)obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), inner.value(), 0);
        }
        else
            assert false : "Unknown condition type: " + obj.getClass().getSimpleName();

        return cnd;
    }

    /** */
    private static Map<ByteArray, Entry> multipleEntryResult(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        Map<ByteArray, Entry> res = new HashMap<>();

        for (SingleEntryResponse e : resp.entries()) {
            ByteArray key = new ByteArray(e.key());

            res.put(key, new EntryImpl(key, e.value(), e.revision(), e.updateCounter()));
        }

        return res;
    }

    /** */
    private static Entry singleEntryResult(Object obj) {
        SingleEntryResponse resp = (SingleEntryResponse) obj;

        return new EntryImpl(new ByteArray(resp.key()), resp.value(), resp.revision(), resp.updateCounter());
    }

    /** */
    private static WatchEvent watchResponse(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        List<EntryEvent> evts = new ArrayList<>(resp.entries().size() / 2);

        Entry o = null;
        Entry n;

        for (int i = 0; i < resp.entries().size(); i++) {
            SingleEntryResponse s = resp.entries().get(i);

            EntryImpl e = new EntryImpl(new ByteArray(s.key()), s.value(), s.revision(), s.updateCounter());

            if (i % 2 == 0)
                o = e;
            else {
                n = e;

                evts.add(new EntryEvent(o, n));
            }
        }

        return new WatchEvent(evts);
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.
    /** Watch processor, that manages {@link Watcher} threads. */
    private final class WatchProcessor {
        /** Active Watcher threads that process notification pulling logic. */
        private final Map<IgniteUuid, Watcher> watchers = new ConcurrentHashMap<>();

        /**
         * Starts exclusive thread per watch that implement watch pulling logic and
         * calls {@link WatchListener#onUpdate(WatchEvent)}} or {@link WatchListener#onError(Throwable)}.
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
                            v.stop = true;

                            Thread.sleep(100);

                            v.cursor.close();
                        }
                        catch (InterruptedException e) {
                            throw new IgniteInternalException(e);
                        }
                        catch (Exception e) {
                            // TODO: IGNITE-14693 Implement Meta storage exception handling logic.
                            LOG.error("Unexpected exception", e);
                        }
                    });
                    return null;
                }
            );
        }

        /** Watcher thread, uses pulling logic in order to retrieve watch notifications from server */
        private final class Watcher extends Thread {
            /** */
            private volatile boolean stop = false;

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
                Iterator<WatchEvent> watchEvtsIter = cursor.iterator();

                while (!stop) {
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

                            lsnr.onUpdate(watchEvt);
                        }
                        else
                            Thread.sleep(10);
                    }
                    catch (Throwable e) {
                        if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException)
                            break;
                        else {
                            // TODO: IGNITE-14693 Implement Meta storage exception handling logic.
                            LOG.error("Unexpected exception", e);
                        }
                    }
                }
            }
        }
    }
}
