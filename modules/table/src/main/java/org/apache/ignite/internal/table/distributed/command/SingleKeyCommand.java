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

package org.apache.ignite.internal.table.distributed.command;

import java.io.Serializable;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.jetbrains.annotations.NotNull;

/**
 * A single key transactional command.
 */
public abstract class SingleKeyCommand implements TransactionalCommand, Serializable {
    /** Binary key row. */
    private transient BinaryRow keyRow;

    /** The timestamp. */
    private @NotNull final Timestamp timestamp;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] keyRowBytes;

    /**
     * The constructor.
     *
     * @param keyRow    The row.
     * @param timestamp The timestamp.
     */
    public SingleKeyCommand(@NotNull BinaryRow keyRow, @NotNull Timestamp timestamp) {
        assert keyRow != null;

        this.keyRow = keyRow;
        this.timestamp = timestamp;

        keyRowBytes = CommandUtils.rowToBytes(keyRow);
    }

    /**
     * Gets a binary key row to be deleted.
     *
     * @return Binary key.
     */
    public BinaryRow getRow() {
        if (keyRow == null) {
            keyRow = new ByteBufferRow(keyRowBytes);
        }

        return keyRow;
    }

    /**
     * Returns a timestamp.
     *
     * @return The timestamp.
     */
    @NotNull
    @Override
    public Timestamp getTimestamp() {
        return timestamp;
    }
}
