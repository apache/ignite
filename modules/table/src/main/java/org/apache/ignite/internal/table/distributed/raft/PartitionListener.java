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

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.Storage;
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
import org.apache.ignite.lang.IgniteInternalException;
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
    private final Storage storage;

    /**
     * Constructor.
     *
     * @param storage Storage.
     */
    public PartitionListener(Storage storage) {
        this.storage = storage;
    }

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            if (clo.command() instanceof GetCommand) {
                DataRow readValue = storage.read(extractAndWrapKey(((GetCommand) clo.command()).getKeyRow()));

                ByteBufferRow responseRow = null;

                if (readValue.hasValueBytes())
                    responseRow = new ByteBufferRow(readValue.valueBytes());

                clo.result(new SingleRowResponse(responseRow));
            }
            else if (clo.command() instanceof GetAllCommand) {
                Set<BinaryRow> keyRows = ((GetAllCommand)clo.command()).getKeyRows();

                assert keyRows != null && !keyRows.isEmpty();

                List<SearchRow> keys = keyRows.stream().map(PartitionListener::extractAndWrapKey)
                    .collect(Collectors.toList());

                List<BinaryRow> res = storage
                    .readAll(keys)
                    .stream()
                    .filter(DataRow::hasValueBytes)
                    .map(read -> new ByteBufferRow(read.valueBytes()))
                    .collect(Collectors.toList());

                clo.result(new MultiRowsResponse(res));
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            if (clo.command() instanceof InsertCommand) {
                BinaryRow row = ((InsertCommand)clo.command()).getRow();

                assert row.hasValue() : "Insert command should have a value.";

                DataRow newRow = extractAndWrapKeyValue(row);

                InsertInvokeClosure writeIfAbsent = new InsertInvokeClosure(newRow);

                storage.invoke(newRow, writeIfAbsent);

                clo.result(writeIfAbsent.result());
            }
            else if (clo.command() instanceof DeleteCommand) {
                SearchRow newRow = extractAndWrapKey(((DeleteCommand)clo.command()).getKeyRow());

                var getAndRemoveClosure = new GetAndRemoveInvokeClosure();

                storage.invoke(newRow, getAndRemoveClosure);

                clo.result(getAndRemoveClosure.result());
            }
            else if (clo.command() instanceof ReplaceCommand) {
                ReplaceCommand cmd = ((ReplaceCommand)clo.command());

                DataRow expected = extractAndWrapKeyValue(cmd.getOldRow());
                DataRow newRow = extractAndWrapKeyValue(cmd.getRow());

                var replaceClosure = new ReplaceExactInvokeClosure(expected, newRow);

                storage.invoke(expected, replaceClosure);

                clo.result(replaceClosure.result());
            }
            else if (clo.command() instanceof UpsertCommand) {
                BinaryRow row = ((UpsertCommand)clo.command()).getRow();

                assert row.hasValue() : "Upsert command should have a value.";

                storage.write(extractAndWrapKeyValue(row));

                clo.result(null);
            }
            else if (clo.command() instanceof InsertAllCommand) {
                Set<BinaryRow> rows = ((InsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                List<DataRow> keyValues = rows.stream().map(PartitionListener::extractAndWrapKeyValue)
                    .collect(Collectors.toList());

                List<BinaryRow> res = storage.insertAll(keyValues).stream()
                    .filter(DataRow::hasValueBytes)
                    .map(inserted -> new ByteBufferRow(inserted.valueBytes()))
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toList());

                clo.result(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof UpsertAllCommand) {
                Set<BinaryRow> rows = ((UpsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                storage.writeAll(rows.stream().map(PartitionListener::extractAndWrapKeyValue).collect(Collectors.toList()));

                clo.result(null);
            }
            else if (clo.command() instanceof DeleteAllCommand) {
                Set<BinaryRow> rows = ((DeleteAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                List<SearchRow> keys = rows.stream().map(PartitionListener::extractAndWrapKey)
                    .collect(Collectors.toList());

                List<BinaryRow> res = storage.removeAll(keys).stream()
                    .filter(DataRow::hasValueBytes)
                    .map(removed -> new ByteBufferRow(removed.valueBytes()))
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toList());

                clo.result(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof DeleteExactCommand) {
                BinaryRow row = ((DeleteExactCommand)clo.command()).getRow();

                assert row != null;
                assert row.hasValue();

                DataRow keyValue = extractAndWrapKeyValue(row);

                var deleteExact = new DeleteExactInvokeClosure(keyValue);

                storage.invoke(keyValue, deleteExact);

                clo.result(deleteExact.result());
            }
            else if (clo.command() instanceof DeleteExactAllCommand) {
                Set<BinaryRow> rows = ((DeleteExactAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                List<DataRow> keyValues = rows.stream().map(PartitionListener::extractAndWrapKeyValue)
                    .collect(Collectors.toList());

                List<BinaryRow> res = storage.removeAllExact(keyValues).stream()
                    .filter(DataRow::hasValueBytes)
                    .map(inserted -> new ByteBufferRow(inserted.valueBytes()))
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toList());

                clo.result(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof ReplaceIfExistCommand) {
                BinaryRow row = ((ReplaceIfExistCommand)clo.command()).getRow();

                assert row != null;

                DataRow keyValue = extractAndWrapKeyValue(row);

                var replaceIfExists = new GetAndReplaceInvokeClosure(keyValue, true);

                storage.invoke(keyValue, replaceIfExists);

                clo.result(replaceIfExists.result());
            }
            else if (clo.command() instanceof GetAndDeleteCommand) {
                BinaryRow row = ((GetAndDeleteCommand)clo.command()).getKeyRow();

                assert row != null;

                SearchRow keyRow = extractAndWrapKey(row);

                var getAndRemoveClosure = new GetAndRemoveInvokeClosure();

                storage.invoke(keyRow, getAndRemoveClosure);

                if (getAndRemoveClosure.result())
                    clo.result(new SingleRowResponse(new ByteBufferRow(getAndRemoveClosure.oldRow().valueBytes())));
                else
                    clo.result(new SingleRowResponse(null));
            }
            else if (clo.command() instanceof GetAndReplaceCommand) {
                BinaryRow row = ((GetAndReplaceCommand)clo.command()).getRow();

                assert row != null && row.hasValue();

                DataRow keyValue = extractAndWrapKeyValue(row);

                var getAndReplace = new GetAndReplaceInvokeClosure(keyValue, true);

                storage.invoke(keyValue, getAndReplace);

                DataRow oldRow = getAndReplace.oldRow();

                BinaryRow res = oldRow.hasValueBytes() ? new ByteBufferRow(oldRow.valueBytes()) : null;

                clo.result(new SingleRowResponse(res));
            }
            else if (clo.command() instanceof GetAndUpsertCommand) {
                BinaryRow row = ((GetAndUpsertCommand)clo.command()).getKeyRow();

                assert row != null && row.hasValue();

                DataRow keyValue = extractAndWrapKeyValue(row);

                var getAndReplace = new GetAndReplaceInvokeClosure(keyValue, false);

                storage.invoke(keyValue, getAndReplace);

                DataRow oldRow = getAndReplace.oldRow();

                if (oldRow.hasValueBytes())
                    clo.result(new SingleRowResponse(new ByteBufferRow(oldRow.valueBytes())));
                else
                    clo.result(new SingleRowResponse(null));
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
     * Extracts a key from the {@link BinaryRow} and wraps it in a {@link SearchRow}.
     *
     * @param row Binary row.
     * @return Search row.
     */
    @NotNull private static SearchRow extractAndWrapKey(@NotNull BinaryRow row) {
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return new SimpleDataRow(key, null);
    }

    /**
     * @return Underlying storage.
     */
    @TestOnly
    public Storage getStorage() {
        return storage;
    }
}
