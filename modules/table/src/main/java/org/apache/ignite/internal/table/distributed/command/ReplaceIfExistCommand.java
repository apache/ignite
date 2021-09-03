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

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command replaces an old entry to a new one.
 */
public class ReplaceIfExistCommand implements WriteCommand {
    /** Binary row. */
    private transient BinaryRow row;

    /**
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowBytes;

    /**
     * Creates a new instance of ReplaceIfExistCommand with the given row to be replaced.
     * The {@code row} should not be {@code null}.
     *
     * @param row Binary row.
     */
    public ReplaceIfExistCommand(@NotNull BinaryRow row) {
        assert row != null;

        this.row = row;

        CommandUtils.rowToBytes(row, bytes -> rowBytes = bytes);
    }

    /**
     * Gets a binary row to be replaced.
     *
     * @return Binary row.
     */
    public BinaryRow getRow() {
        if (row == null)
            row = new ByteBufferRow(rowBytes);

        return row;
    }
}
