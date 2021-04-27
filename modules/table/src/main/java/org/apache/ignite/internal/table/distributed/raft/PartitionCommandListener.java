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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.KVGetResponse;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.jetbrains.annotations.NotNull;

/**
 * Partition command handler.
 */
public class PartitionCommandListener implements RaftGroupCommandListener {
    /** Storage. */
    private ConcurrentHashMap<KeyWrapper, BinaryRow> storage = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            assert clo.command() instanceof GetCommand;

            clo.success(new KVGetResponse(storage.get(extractAndWrapKey(((GetCommand)clo.command()).getKeyRow()))));
        }
    }

    /** {@inheritDoc} */
    @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            if (clo.command() instanceof InsertCommand) {
                BinaryRow previous = storage.putIfAbsent(
                    extractAndWrapKey(((InsertCommand)clo.command()).getRow()),
                    ((InsertCommand)clo.command()).getRow()
                );

                clo.success(previous == null);
            }
            else if (clo.command() instanceof DeleteCommand) {
                BinaryRow deleted = storage.remove(
                    extractAndWrapKey(((DeleteCommand)clo.command()).getKeyRow())
                );

                clo.success(deleted != null);
            }
            else if (clo.command() instanceof ReplaceCommand) {
                ReplaceCommand cmd = ((ReplaceCommand)clo.command());

                BinaryRow expected = cmd.getOldRow();

                KeyWrapper key = extractAndWrapKey(expected);

                BinaryRow current = storage.get(key);

                if ((current == null && !expected.hasValue()) ||
                    equalValues(current, expected)) {
                    storage.put(key, cmd.getRow());

                    clo.success(true);
                }
                else
                    clo.success(false);
            }
            else if (clo.command() instanceof UpsertCommand) {
                storage.put(
                    extractAndWrapKey(((UpsertCommand)clo.command()).getRow()),
                    ((UpsertCommand)clo.command()).getRow()
                );

                clo.success(null);
            }
            else
                assert false : "Command was not found [cmd=" + clo.command() + ']';
        }
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
        if (row.hasValue() ^ row2.hasValue())
            return false;

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
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
}
