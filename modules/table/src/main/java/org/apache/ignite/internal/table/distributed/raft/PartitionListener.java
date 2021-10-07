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

package org.apache.ignite.internal.table.distributed.raft;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.DeleteExactInvokeClosure;
import org.apache.ignite.internal.storage.basic.GetAndRemoveInvokeClosure;
import org.apache.ignite.internal.storage.basic.GetAndReplaceInvokeClosure;
import org.apache.ignite.internal.storage.basic.InsertInvokeClosure;
import org.apache.ignite.internal.storage.basic.ReplaceExactInvokeClosure;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceIfExistCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.table.distributed.command.scan.ScanCloseCommand;
import org.apache.ignite.internal.table.distributed.command.scan.ScanInitCommand;
import org.apache.ignite.internal.table.distributed.command.scan.ScanRetrieveBatchCommand;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener {
    /** Partition storage. */
    private final PartitionStorage storage;

    /** Cursors map. */
    private final Map<IgniteUuid, CursorMeta> cursors;

    /**
     * Constructor.
     *
     * @param partitionStorage Storage.
     */
    public PartitionListener(PartitionStorage partitionStorage) {
        this.storage = partitionStorage;
        this.cursors = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends ReadCommand> clo) -> {
            if (clo.command() instanceof GetCommand)
                handleGetCommand((CommandClosure<GetCommand>) clo);
            else if (clo.command() instanceof GetAllCommand)
                handleGetAllCommand((CommandClosure<GetAllCommand>) clo);
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        });
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        iterator.forEachRemaining((CommandClosure<? extends WriteCommand> clo) -> {
            Command command = clo.command();

            if (command instanceof InsertCommand)
                handleInsertCommand((CommandClosure<InsertCommand>) clo);
            else if (command instanceof DeleteCommand)
                handleDeleteCommand((CommandClosure<DeleteCommand>) clo);
            else if (command instanceof ReplaceCommand)
                handleReplaceCommand((CommandClosure<ReplaceCommand>) clo);
            else if (command instanceof UpsertCommand)
                handleUpsertCommand((CommandClosure<UpsertCommand>) clo);
            else if (command instanceof InsertAllCommand)
                handleInsertAllCommand((CommandClosure<InsertAllCommand>) clo);
            else if (command instanceof UpsertAllCommand)
                handleUpsertAllCommand((CommandClosure<UpsertAllCommand>) clo);
            else if (command instanceof DeleteAllCommand)
                handleDeleteAllCommand((CommandClosure<DeleteAllCommand>) clo);
            else if (command instanceof DeleteExactCommand)
                handleDeleteExactCommand((CommandClosure<DeleteExactCommand>) clo);
            else if (command instanceof DeleteExactAllCommand)
                handleDeleteExactAllCommand((CommandClosure<DeleteExactAllCommand>) clo);
            else if (command instanceof ReplaceIfExistCommand)
                handleReplaceIfExistsCommand((CommandClosure<ReplaceIfExistCommand>) clo);
            else if (command instanceof GetAndDeleteCommand)
                handleGetAndDeleteCommand((CommandClosure<GetAndDeleteCommand>) clo);
            else if (command instanceof GetAndReplaceCommand)
                handleGetAndReplaceCommand((CommandClosure<GetAndReplaceCommand>) clo);
            else if (command instanceof GetAndUpsertCommand)
                handleGetAndUpsertCommand((CommandClosure<GetAndUpsertCommand>) clo);
            else if (command instanceof ScanInitCommand)
                handleScanInitCommand((CommandClosure<ScanInitCommand>) clo);
            else if (command instanceof ScanRetrieveBatchCommand)
                handleScanRetrieveBatchCommand((CommandClosure<ScanRetrieveBatchCommand>) clo);
            else if (command instanceof ScanCloseCommand)
                handleScanCloseCommand((CommandClosure<ScanCloseCommand>) clo);
            else
                assert false : "Command was not found [cmd=" + command + ']';
        });
    }

    /**
     * Handler for the {@link GetCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetCommand(CommandClosure<GetCommand> clo) {
        BinaryRow keyRow = clo.command().getKeyRow();

        DataRow readValue = storage.read(new BinarySearchRow(keyRow));

        BinaryRow responseRow = readValue == null ? null : new ByteBufferRow(readValue.valueBytes());

        clo.result(new SingleRowResponse(responseRow));
    }

    /**
     * Handler for the {@link GetAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAllCommand(CommandClosure<GetAllCommand> clo) {
        Set<BinaryRow> keyRows = clo.command().getKeyRows();

        assert keyRows != null && !keyRows.isEmpty();

        List<SearchRow> keys = keyRows.stream()
            .map(BinarySearchRow::new)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.readAll(keys).stream()
            .map(read -> new ByteBufferRow(read.valueBytes()))
            .collect(Collectors.toList());

        clo.result(new MultiRowsResponse(res));
    }

    /**
     * Handler for the {@link InsertCommand}.
     *
     * @param clo Command closure.
     */
    private void handleInsertCommand(CommandClosure<InsertCommand> clo) {
        BinaryRow row = clo.command().getRow();

        assert row.hasValue() : "Insert command should have a value.";

        DataRow newRow = extractAndWrapKeyValue(row);

        var writeIfAbsent = new InsertInvokeClosure(newRow);

        storage.invoke(newRow, writeIfAbsent);

        clo.result(writeIfAbsent.result());
    }

    /**
     * Handler for the {@link DeleteCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteCommand(CommandClosure<DeleteCommand> clo) {
        BinaryRow keyRow = clo.command().getKeyRow();

        SearchRow newRow = new BinarySearchRow(keyRow);

        var getAndRemoveClosure = new GetAndRemoveInvokeClosure();

        storage.invoke(newRow, getAndRemoveClosure);

        clo.result(getAndRemoveClosure.result());
    }

    /**
     * Handler for the {@link ReplaceCommand}.
     *
     * @param clo Command closure.
     */
    private void handleReplaceCommand(CommandClosure<ReplaceCommand> clo) {
        DataRow expected = extractAndWrapKeyValue(clo.command().getOldRow());
        DataRow newRow = extractAndWrapKeyValue(clo.command().getRow());

        var replaceClosure = new ReplaceExactInvokeClosure(expected, newRow);

        storage.invoke(expected, replaceClosure);

        clo.result(replaceClosure.result());
    }

    /**
     * Handler for the {@link UpsertCommand}.
     *
     * @param clo Command closure.
     */
    private void handleUpsertCommand(CommandClosure<UpsertCommand> clo) {
        BinaryRow row = clo.command().getRow();

        assert row.hasValue() : "Upsert command should have a value.";

        storage.write(extractAndWrapKeyValue(row));

        clo.result(null);
    }

    /**
     * Handler for the {@link InsertAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleInsertAllCommand(CommandClosure<InsertAllCommand> clo) {
        Set<BinaryRow> rows = clo.command().getRows();

        assert rows != null && !rows.isEmpty();

        List<DataRow> keyValues = rows.stream()
            .map(PartitionListener::extractAndWrapKeyValue)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.insertAll(keyValues).stream()
            .map(skipped -> new ByteBufferRow(skipped.valueBytes()))
            .collect(Collectors.toList());

        clo.result(new MultiRowsResponse(res));
    }

    /**
     * Handler for the {@link UpsertAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleUpsertAllCommand(CommandClosure<UpsertAllCommand> clo) {
        Set<BinaryRow> rows = clo.command().getRows();

        assert rows != null && !rows.isEmpty();

        List<DataRow> keyValues = rows.stream()
            .map(PartitionListener::extractAndWrapKeyValue)
            .collect(Collectors.toList());

        storage.writeAll(keyValues);

        clo.result(null);
    }

    /**
     * Handler for the {@link DeleteAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteAllCommand(CommandClosure<DeleteAllCommand> clo) {
        Set<BinaryRow> rows = clo.command().getRows();

        assert rows != null && !rows.isEmpty();

        List<SearchRow> keys = rows.stream()
            .map(BinarySearchRow::new)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.removeAll(keys).stream()
            .map(skipped -> ((BinarySearchRow)skipped).sourceRow)
            .collect(Collectors.toList());

        clo.result(new MultiRowsResponse(res));
    }

    /**
     * Handler for the {@link DeleteExactCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteExactCommand(CommandClosure<DeleteExactCommand> clo) {
        BinaryRow row = clo.command().getRow();

        assert row != null && row.hasValue();

        DataRow keyValue = extractAndWrapKeyValue(row);

        var deleteExact = new DeleteExactInvokeClosure(keyValue);

        storage.invoke(keyValue, deleteExact);

        clo.result(deleteExact.result());
    }

    /**
     * Handler for the {@link DeleteExactAllCommand}.
     *
     * @param clo Command closure.
     */
    private void handleDeleteExactAllCommand(CommandClosure<DeleteExactAllCommand> clo) {
        Set<BinaryRow> rows = clo.command().getRows();

        assert rows != null && !rows.isEmpty();

        List<DataRow> keyValues = rows.stream()
            .map(PartitionListener::extractAndWrapKeyValue)
            .collect(Collectors.toList());

        List<BinaryRow> res = storage.removeAllExact(keyValues).stream()
            .map(skipped -> new ByteBufferRow(skipped.valueBytes()))
            .collect(Collectors.toList());

        clo.result(new MultiRowsResponse(res));
    }

    /**
     * Handler for the {@link ReplaceIfExistCommand}.
     *
     * @param clo Command closure.
     */
    private void handleReplaceIfExistsCommand(CommandClosure<ReplaceIfExistCommand> clo) {
        BinaryRow row = clo.command().getRow();

        assert row != null;

        DataRow keyValue = extractAndWrapKeyValue(row);

        var replaceIfExists = new GetAndReplaceInvokeClosure(keyValue, true);

        storage.invoke(keyValue, replaceIfExists);

        clo.result(replaceIfExists.result());
    }

    /**
     * Handler for the {@link GetAndDeleteCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAndDeleteCommand(CommandClosure<GetAndDeleteCommand> clo) {
        BinaryRow row = clo.command().getKeyRow();

        assert row != null;

        SearchRow keyRow = new BinarySearchRow(row);

        var getAndRemoveClosure = new GetAndRemoveInvokeClosure();

        storage.invoke(keyRow, getAndRemoveClosure);

        BinaryRow removedRow = getAndRemoveClosure.result() ?
            new ByteBufferRow(getAndRemoveClosure.oldRow().valueBytes()) :
            null;

        clo.result(new SingleRowResponse(removedRow));
    }

    /**
     * Handler for the {@link GetAndReplaceCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAndReplaceCommand(CommandClosure<GetAndReplaceCommand> clo) {
        BinaryRow row = clo.command().getRow();

        assert row != null && row.hasValue();

        DataRow keyValue = extractAndWrapKeyValue(row);

        var getAndReplace = new GetAndReplaceInvokeClosure(keyValue, true);

        storage.invoke(keyValue, getAndReplace);

        DataRow oldRow = getAndReplace.oldRow();

        BinaryRow res = oldRow == null ? null : new ByteBufferRow(oldRow.valueBytes());

        clo.result(new SingleRowResponse(res));
    }

    /**
     * Handler for the {@link GetAndUpsertCommand}.
     *
     * @param clo Command closure.
     */
    private void handleGetAndUpsertCommand(CommandClosure<GetAndUpsertCommand> clo) {
        BinaryRow row = clo.command().getKeyRow();

        assert row != null && row.hasValue();

        DataRow keyValue = extractAndWrapKeyValue(row);

        var getAndReplace = new GetAndReplaceInvokeClosure(keyValue, false);

        storage.invoke(keyValue, getAndReplace);

        DataRow oldRow = getAndReplace.oldRow();

        BinaryRow response = oldRow == null ? null : new ByteBufferRow(oldRow.valueBytes());

        clo.result(new SingleRowResponse(response));
    }

    /**
     * Handler for the {@link ScanInitCommand}.
     *
     * @param clo Command closure.
     */
    private void handleScanInitCommand(CommandClosure<ScanInitCommand> clo) {
        ScanInitCommand rangeCmd = clo.command();

        IgniteUuid cursorId = rangeCmd.scanId();

        try {
            Cursor<DataRow> cursor = storage.scan(key -> true);

            cursors.put(
                cursorId,
                new CursorMeta(
                    cursor,
                    rangeCmd.requesterNodeId()
                )
            );
        }
        catch (StorageException e) {
            clo.result(e);
        }

        clo.result(null);
    }

    /**
     * Handler for the {@link ScanRetrieveBatchCommand}.
     *
     * @param clo Command closure.
     */
    private void handleScanRetrieveBatchCommand(CommandClosure<ScanRetrieveBatchCommand> clo) {
        CursorMeta cursorDesc = cursors.get(clo.command().scanId());

        if (cursorDesc == null) {
            clo.result(new NoSuchElementException(LoggerMessageHelper.format(
                "Cursor with id={} is not found on server side.", clo.command().scanId())));
        }

        List<BinaryRow> res = new ArrayList<>();

        try {
            for (int i = 0; i < clo.command().itemsToRetrieveCount() && cursorDesc.cursor().hasNext(); i++)
                res.add(new ByteBufferRow(cursorDesc.cursor().next().valueBytes()));
        }
        catch (Exception e) {
            clo.result(e);
        }

        clo.result(new MultiRowsResponse(res));
    }

    /**
     * Handler for the {@link ScanCloseCommand}.
     *
     * @param clo Command closure.
     */
    private void handleScanCloseCommand(CommandClosure<ScanCloseCommand> clo) {
        CursorMeta cursorDesc = cursors.remove(clo.command().scanId());

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
     * Extracts a key and a value from the {@link BinaryRow} and wraps it in a {@link DataRow}.
     *
     * @param row Binary row.
     * @return Data row.
     */
    @NotNull private static DataRow extractAndWrapKeyValue(@NotNull BinaryRow row) {
        byte[] key = new byte[row.keySlice().capacity()];

        row.keySlice().get(key);

        return new SimpleDataRow(key, row.bytes());
    }

    /**
     * Adapter that converts a {@link BinaryRow} into a {@link SearchRow}.
     */
    private static class BinarySearchRow implements SearchRow {
        /** Search key. */
        private final byte[] keyBytes;

        /** Source row. */
        private final BinaryRow sourceRow;

        /**
         * Constructor.
         *
         * @param row Row to search for.
         */
        BinarySearchRow(BinaryRow row) {
            sourceRow = row;
            keyBytes = new byte[row.keySlice().capacity()];

            row.keySlice().get(keyBytes);
        }

        /** {@inheritDoc} */
        @Override public byte @NotNull [] keyBytes() {
            return keyBytes;
        }

        /** {@inheritDoc} */
        @Override public @NotNull ByteBuffer key() {
            return ByteBuffer.wrap(keyBytes);
        }
    }

    /**
     * @return Underlying storage.
     */
    @TestOnly
    public PartitionStorage getStorage() {
        return storage;
    }

    /**
     * Cursor meta information: origin node id and type.
     */
    private class CursorMeta {
        /** Cursor. */
        private final Cursor<DataRow> cursor;

        /** Id of the node that creates cursor. */
        private final String requesterNodeId;

        /**
         * The constructor.
         *
         * @param cursor Cursor.
         * @param requesterNodeId Id of the node that creates cursor.
         */
        CursorMeta(
            Cursor<DataRow> cursor,
            String requesterNodeId
        ) {
            this.cursor = cursor;
            this.requesterNodeId = requesterNodeId;
        }

        /**
         * @return Cursor.
         */
        public Cursor<DataRow> cursor() {
            return cursor;
        }

        /**
         * @return Id of the node that creates cursor.
         */
        public String requesterNodeId() {
            return requesterNodeId;
        }
    }
}
