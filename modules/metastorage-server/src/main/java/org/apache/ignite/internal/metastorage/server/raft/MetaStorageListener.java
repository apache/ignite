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

package org.apache.ignite.internal.metastorage.server.raft;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.metastorage.common.ConditionType;
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
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorCloseCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorHasNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorsCloseCommand;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.EntryEvent;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.Operation;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.TombstoneCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition;
import org.apache.ignite.internal.metastorage.server.WatchEvent;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.TestOnly;

/**
 * Meta storage listener.
 * TODO: IGNITE-14693 Implement Meta storage exception handling logic.
 */
public class MetaStorageListener implements RaftGroupListener {
    /** Storage. */
    private final KeyValueStorage storage;

    /** Cursors map. */
    private final Map<IgniteUuid, CursorMeta> cursors;

    /**
     * @param storage Storage.
     */
    public MetaStorageListener(KeyValueStorage storage) {
        this.storage = storage;
        this.cursors = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<ReadCommand> clo = iter.next();

            if (clo.command() instanceof GetCommand) {
                GetCommand getCmd = (GetCommand) clo.command();

                Entry e;

                if (getCmd.revision() != 0)
                    e = storage.get(getCmd.key(), getCmd.revision());
                else
                    e = storage.get(getCmd.key());

                SingleEntryResponse resp = new SingleEntryResponse(
                    e.key(), e.value(), e.revision(), e.updateCounter()
                );

                clo.result(resp);
            }
            else if (clo.command() instanceof GetAllCommand) {
                GetAllCommand getAllCmd = (GetAllCommand) clo.command();

                Collection<Entry> entries;

                if (getAllCmd.revision() != 0)
                    entries = storage.getAll(getAllCmd.keys(), getAllCmd.revision());
                else
                    entries = storage.getAll(getAllCmd.keys());

                List<SingleEntryResponse> res = new ArrayList<>(entries.size());

                for (Entry e : entries)
                    res.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));

                clo.result(new MultipleEntryResponse(res));
            }
            else if (clo.command() instanceof CursorHasNextCommand) {
                CursorHasNextCommand cursorHasNextCmd = (CursorHasNextCommand) clo.command();

                CursorMeta cursorDesc = cursors.get(cursorHasNextCmd.cursorId());

                clo.result(!(cursorDesc == null) && cursorDesc.cursor().hasNext());
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<WriteCommand> clo = iter.next();

            if (clo.command() instanceof PutCommand) {
                PutCommand putCmd = (PutCommand) clo.command();

                storage.put(putCmd.key(), putCmd.value());

                clo.result(null);
            }
            else if (clo.command() instanceof GetAndPutCommand) {
                GetAndPutCommand getAndPutCmd = (GetAndPutCommand) clo.command();

                Entry e = storage.getAndPut(getAndPutCmd.key(), getAndPutCmd.value());

                clo.result(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
            }
            else if (clo.command() instanceof PutAllCommand) {
                PutAllCommand putAllCmd = (PutAllCommand) clo.command();

                storage.putAll(putAllCmd.keys(), putAllCmd.values());

                clo.result(null);
            }
            else if (clo.command() instanceof GetAndPutAllCommand) {
                GetAndPutAllCommand getAndPutAllCmd = (GetAndPutAllCommand) clo.command();

                Collection<Entry> entries = storage.getAndPutAll(getAndPutAllCmd.keys(), getAndPutAllCmd.vals());

                List<SingleEntryResponse> resp = new ArrayList<>(entries.size());

                for (Entry e : entries)
                    resp.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));

                clo.result(new MultipleEntryResponse(resp));
            }
            else if (clo.command() instanceof RemoveCommand) {
                RemoveCommand rmvCmd = (RemoveCommand) clo.command();

                storage.remove(rmvCmd.key());

                clo.result(null);
            }
            else if (clo.command() instanceof GetAndRemoveCommand) {
                GetAndRemoveCommand getAndRmvCmd = (GetAndRemoveCommand) clo.command();

                Entry e = storage.getAndRemove(getAndRmvCmd.key());

                clo.result(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
            }
            else if (clo.command() instanceof RemoveAllCommand) {
                RemoveAllCommand rmvAllCmd = (RemoveAllCommand) clo.command();

                storage.removeAll(rmvAllCmd.keys());

                clo.result(null);
            }
            else if (clo.command() instanceof GetAndRemoveAllCommand) {
                GetAndRemoveAllCommand getAndRmvAllCmd = (GetAndRemoveAllCommand) clo.command();

                Collection<Entry> entries = storage.getAndRemoveAll(getAndRmvAllCmd.keys());

                List<SingleEntryResponse> resp = new ArrayList<>(entries.size());

                for (Entry e : entries)
                    resp.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));

                clo.result(new MultipleEntryResponse(resp));
            }
            else if (clo.command() instanceof InvokeCommand) {
                InvokeCommand cmd = (InvokeCommand) clo.command();

                boolean res = storage.invoke(
                    toCondition(cmd.condition()),
                    toOperations(cmd.success()),
                    toOperations(cmd.failure())
                );

                clo.result(res);
            }
            else if (clo.command() instanceof RangeCommand) {
                RangeCommand rangeCmd = (RangeCommand) clo.command();

                IgniteUuid cursorId = rangeCmd.getCursorId();

                Cursor<Entry> cursor = (rangeCmd.revUpperBound() != -1) ?
                    storage.range(
                        rangeCmd.keyFrom(),
                        rangeCmd.keyTo(),
                        rangeCmd.revUpperBound()) :
                    storage.range(
                        rangeCmd.keyFrom(),
                        rangeCmd.keyTo());

                cursors.put(
                    cursorId,
                    new CursorMeta(
                        cursor,
                        CursorType.RANGE,
                        rangeCmd.requesterNodeId()
                    )
                );

                clo.result(cursorId);
            }
            else if (clo.command() instanceof CursorNextCommand) {
                CursorNextCommand cursorNextCmd = (CursorNextCommand) clo.command();

                CursorMeta cursorDesc = cursors.get(cursorNextCmd.cursorId());

                if (cursorDesc == null) {
                    clo.result(new NoSuchElementException("Corresponding cursor on server side not found."));

                    return;
                }

                if (cursorDesc.type() == CursorType.RANGE) {
                    Entry e = (Entry) cursorDesc.cursor().next();

                    clo.result(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
                }
                else if (cursorDesc.type() == CursorType.WATCH) {
                    WatchEvent evt = (WatchEvent) cursorDesc.cursor().next();

                    List<SingleEntryResponse> resp = new ArrayList<>(evt.entryEvents().size() * 2);

                    for (EntryEvent e : evt.entryEvents()) {
                        Entry o = e.oldEntry();

                        Entry n = e.entry();

                        resp.add(new SingleEntryResponse(o.key(), o.value(), o.revision(), o.updateCounter()));

                        resp.add(new SingleEntryResponse(n.key(), n.value(), n.revision(), n.updateCounter()));
                    }

                    clo.result(new MultipleEntryResponse(resp));
                }
            }
            else if (clo.command() instanceof CursorCloseCommand) {
                CursorCloseCommand cursorCloseCmd = (CursorCloseCommand) clo.command();

                CursorMeta cursorDesc = cursors.get(cursorCloseCmd.cursorId());

                if (cursorDesc == null) {
                    clo.result(null);

                    return;
                }

                try {
                    cursorDesc.cursor().close();
                }
                catch (Exception e) {
                    throw new IgniteInternalException(e);
                }

                clo.result(null);
            }
            else if (clo.command() instanceof WatchRangeKeysCommand) {
                WatchRangeKeysCommand watchCmd = (WatchRangeKeysCommand) clo.command();

                IgniteUuid cursorId = watchCmd.getCursorId();

                Cursor<WatchEvent> cursor =
                    storage.watch(watchCmd.keyFrom(), watchCmd.keyTo(), watchCmd.revision());

                cursors.put(
                    cursorId,
                    new CursorMeta(
                        cursor,
                        CursorType.WATCH,
                        watchCmd.requesterNodeId()
                    )
                );

                clo.result(cursorId);
            }
            else if (clo.command() instanceof WatchExactKeysCommand) {
                WatchExactKeysCommand watchCmd = (WatchExactKeysCommand) clo.command();

                IgniteUuid cursorId = watchCmd.getCursorId();

                Cursor<WatchEvent> cursor = storage.watch(watchCmd.keys(), watchCmd.revision());

                cursors.put(
                    cursorId,
                    new CursorMeta(
                        cursor,
                        CursorType.WATCH,
                        watchCmd.requesterNodeId()
                    )
                );

                clo.result(cursorId);
            }
            else if (clo.command() instanceof CursorsCloseCommand) {
                CursorsCloseCommand cursorsCloseCmd = (CursorsCloseCommand) clo.command();

                Iterator<CursorMeta> cursorsIter = cursors.values().iterator();

                while (cursorsIter.hasNext()) {
                    CursorMeta cursorDesc = cursorsIter.next();

                    if (cursorDesc.requesterNodeId().equals(cursorsCloseCmd.nodeId())) {
                        try {
                            cursorDesc.cursor().close();
                        }
                        catch (Exception e) {
                            throw new IgniteInternalException(e);
                        }

                        cursorsIter.remove();
                    }

                }

                clo.result(null);
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
    }

    /** {@inheritDoc} */
    @Override public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path).whenComplete((unused, throwable) -> {
            doneClo.accept(throwable);
        });
    }

    /** {@inheritDoc} */
    @Override public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);
        return true;
    }

    /** {@inheritDoc} */
    @Override public void onShutdown() {
        try {
            storage.close();
        }
        catch (Exception e) {
            throw new IgniteInternalException("Failed to close storage: " + e.getMessage(), e);
        }
    }

    /**
     * @return {@link KeyValueStorage} that is backing this listener.
     */
    @TestOnly
    public KeyValueStorage getStorage() {
        return storage;
    }

    /** */
    private static Condition toCondition(ConditionInfo info) {
        byte[] key = info.key();

        ConditionType type = info.type();

        if (type == ConditionType.KEY_EXISTS)
            return new ExistenceCondition(ExistenceCondition.Type.EXISTS, key);
        else if (type == ConditionType.KEY_NOT_EXISTS)
            return new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, key);
        else if (type == ConditionType.TOMBSTONE)
            return new TombstoneCondition(key);
        else if (type == ConditionType.VAL_EQUAL)
            return new ValueCondition(ValueCondition.Type.EQUAL, key, info.value());
        else if (type == ConditionType.VAL_NOT_EQUAL)
            return new ValueCondition(ValueCondition.Type.NOT_EQUAL, key, info.value());
        else if (type == ConditionType.REV_EQUAL)
            return new RevisionCondition(RevisionCondition.Type.EQUAL, key, info.revision());
        else if (type == ConditionType.REV_NOT_EQUAL)
            return new RevisionCondition(RevisionCondition.Type.NOT_EQUAL, key, info.revision());
        else if (type == ConditionType.REV_GREATER)
            return new RevisionCondition(RevisionCondition.Type.GREATER, key, info.revision());
        else if (type == ConditionType.REV_GREATER_OR_EQUAL)
            return new RevisionCondition(RevisionCondition.Type.GREATER_OR_EQUAL, key, info.revision());
        else if (type == ConditionType.REV_LESS)
            return new RevisionCondition(RevisionCondition.Type.LESS, key, info.revision());
        else if (type == ConditionType.REV_LESS_OR_EQUAL)
            return new RevisionCondition(RevisionCondition.Type.LESS_OR_EQUAL, key, info.revision());
        else
            throw new IllegalArgumentException("Unknown condition type: " + type);
    }

    /** */
    private static List<Operation> toOperations(List<OperationInfo> infos) {
        List<Operation> ops = new ArrayList<>(infos.size());

        for (OperationInfo info : infos)
            ops.add(new Operation(info.type(), info.key(), info.value()));

        return ops;
    }

    /**
     * Cursor meta information: origin node id and type.
     */
    private class CursorMeta {
        /** Cursor. */
        private final Cursor<?> cursor;

        /** Cursor type. */
        private final CursorType type;

        /** Id of the node that creates cursor. */
        private final String requesterNodeId;

        /**
         * The constructor.
         *
         * @param cursor Cursor.
         * @param type Cursor type.
         * @param requesterNodeId Id of the node that creates cursor.
         */
        CursorMeta(Cursor<?> cursor,
            CursorType type,
            String requesterNodeId
        ) {
            this.cursor = cursor;
            this.type = type;
            this.requesterNodeId = requesterNodeId;
        }

        /**
         * @return Cursor.
         */
        public Cursor<?> cursor() {
            return cursor;
        }

        /**
         * @return Cursor type.
         */
        public CursorType type() {
            return type;
        }

        /**
         * @return Id of the node that creates cursor.
         */
        public String requesterNodeId() {
            return requesterNodeId;
        }
    }

    /** Cursor type. */
    private enum CursorType {
        RANGE,

        WATCH;
    }
}
