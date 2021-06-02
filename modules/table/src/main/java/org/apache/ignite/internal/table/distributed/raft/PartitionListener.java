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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
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
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.NotNull;

/**
 * Partition command handler.
 */
public class PartitionListener implements RaftGroupListener {
    /**
     * Storage.
     * This is a temporary solution, it will apply until persistence layer would not be implemented.
     * TODO: IGNITE-14790.
     */
    private ConcurrentHashMap<KeyWrapper, BinaryRow> storage = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            if (clo.command() instanceof GetCommand) {
                clo.result(new SingleRowResponse(storage.get(
                    extractAndWrapKey(((GetCommand)clo.command()).getKeyRow())
                )));
            }
            else if (clo.command() instanceof GetAllCommand) {
                Set<BinaryRow> keyRows = ((GetAllCommand)clo.command()).getKeyRows();

                assert keyRows != null && !keyRows.isEmpty();

                final Set<BinaryRow> res = keyRows.stream()
                    .map(this::extractAndWrapKey)
                    .map(storage::get)
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

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

                BinaryRow previous = storage.putIfAbsent(extractAndWrapKey(row), row);

                clo.result(previous == null);
            }
            else if (clo.command() instanceof DeleteCommand) {
                BinaryRow deleted = storage.remove(
                    extractAndWrapKey(((DeleteCommand)clo.command()).getKeyRow())
                );

                clo.result(deleted != null);
            }
            else if (clo.command() instanceof ReplaceCommand) {
                ReplaceCommand cmd = ((ReplaceCommand)clo.command());

                BinaryRow expected = cmd.getOldRow();

                KeyWrapper key = extractAndWrapKey(expected);

                BinaryRow current = storage.get(key);

                if ((current == null && !expected.hasValue()) ||
                    equalValues(current, expected)) {
                    storage.put(key, cmd.getRow());

                    clo.result(true);
                }
                else
                    clo.result(false);
            }
            else if (clo.command() instanceof UpsertCommand) {
                BinaryRow row = ((UpsertCommand)clo.command()).getRow();

                assert row.hasValue() : "Upsert command should have a value.";

                storage.put(extractAndWrapKey(row), row);

                clo.result(null);
            }
            else if (clo.command() instanceof InsertAllCommand) {
                Set<BinaryRow> rows = ((InsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                final Set<BinaryRow> res = rows.stream()
                    .map(k -> storage.putIfAbsent(extractAndWrapKey(k), k) == null ? null : k)
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

                clo.result(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof UpsertAllCommand) {
                Set<BinaryRow> rows = ((UpsertAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                rows.forEach(k -> storage.put(extractAndWrapKey(k), k));

                clo.result(null);
            }
            else if (clo.command() instanceof DeleteAllCommand) {
                Set<BinaryRow> rows = ((DeleteAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                final Set<BinaryRow> res = rows.stream()
                    .map(k -> {
                        if (k.hasValue())
                            return null;
                        else
                            return storage.remove(extractAndWrapKey(k));
                    })
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

                clo.result(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof DeleteExactCommand) {
                BinaryRow row = ((DeleteExactCommand)clo.command()).getRow();

                assert row != null;
                assert row.hasValue();

                final KeyWrapper key = extractAndWrapKey(row);
                final BinaryRow old = storage.get(key);

                if (old == null || !old.hasValue())
                    clo.result(false);
                else
                    clo.result(equalValues(row, old) && storage.remove(key) != null);
            }
            else if (clo.command() instanceof DeleteExactAllCommand) {
                Set<BinaryRow> rows = ((DeleteExactAllCommand)clo.command()).getRows();

                assert rows != null && !rows.isEmpty();

                final Set<BinaryRow> res = rows.stream()
                    .map(k -> {
                        final KeyWrapper key = extractAndWrapKey(k);
                        final BinaryRow old = storage.get(key);

                        if (old == null || !old.hasValue() || !equalValues(k, old))
                            return null;

                        return storage.remove(key);
                    })
                    .filter(Objects::nonNull)
                    .filter(BinaryRow::hasValue)
                    .collect(Collectors.toSet());

                clo.result(new MultiRowsResponse(res));
            }
            else if (clo.command() instanceof ReplaceIfExistCommand) {
                BinaryRow row = ((ReplaceIfExistCommand)clo.command()).getRow();

                assert row != null;

                final KeyWrapper key = extractAndWrapKey(row);
                final BinaryRow oldRow = storage.get(key);

                if (oldRow == null || !oldRow.hasValue())
                    clo.result(false);
                else
                    clo.result(storage.put(key, row) == oldRow);
            }
            else if (clo.command() instanceof GetAndDeleteCommand) {
                BinaryRow row = ((GetAndDeleteCommand)clo.command()).getKeyRow();

                assert row != null;

                BinaryRow oldRow = storage.remove(extractAndWrapKey(row));

                if (oldRow == null || !oldRow.hasValue())
                    clo.result(new SingleRowResponse(null));
                else
                    clo.result(new SingleRowResponse(oldRow));
            }
            else if (clo.command() instanceof GetAndReplaceCommand) {
                BinaryRow row = ((GetAndReplaceCommand)clo.command()).getRow();

                assert row != null && row.hasValue();

                BinaryRow oldRow = storage.get(extractAndWrapKey(row));

                storage.computeIfPresent(extractAndWrapKey(row), (key, val) -> row);

                if (oldRow == null || !oldRow.hasValue())
                    clo.result(new SingleRowResponse(null));
                else
                    clo.result(new SingleRowResponse(oldRow));
            }
            else if (clo.command() instanceof GetAndUpsertCommand) {
                BinaryRow row = ((GetAndUpsertCommand)clo.command()).getKeyRow();

                assert row != null && row.hasValue();

                BinaryRow oldRow = storage.put(extractAndWrapKey(row), row);

                if (oldRow == null || !oldRow.hasValue())
                    clo.result(new SingleRowResponse(null));
                else
                    clo.result(new SingleRowResponse(oldRow));
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
    }

    /** {@inheritDoc} */
    @Override public void onSnapshotSave(String path, Consumer<Throwable> doneClo) {
        // Not implemented yet.
    }

    /** {@inheritDoc} */
    @Override public boolean onSnapshotLoad(String path) {
        // Not implemented yet.
        return false;
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    private static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyWrapper wrapper = (KeyWrapper)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }

    /**
     * Compares two rows.
     *
     * @param row Row to compare.
     * @param row2 Row to compare.
     * @return True if these rows is equivalent, false otherwise.
     */
    private boolean equalValues(BinaryRow row, BinaryRow row2) {
        if (row == row2)
            return true;

        if (row == null || row2 == null)
            return false;

        if (row.hasValue() ^ row2.hasValue())
            return false;

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * Makes a wrapped key from a table row.
     *
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
    }
}
