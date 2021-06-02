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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command puts a batch rows.
 */
public class UpsertAllCommand implements WriteCommand {
    /** Binary rows. */
    private transient Set<BinaryRow> rows;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after (IGNITE-14793).
     */
    private byte[] rowsBytes;

    /**
     * Creates a new instance of UpsertAllCommand with the given rows to be upserted.
     * The {@code rows} should not be {@code null} or empty.
     *
     * @param rows Binary rows.
     */
    public UpsertAllCommand(@NotNull Set<BinaryRow> rows) {
        assert rows != null && !rows.isEmpty();

        this.rows = rows;

        CommandUtils.rowsToBytes(rows, bytes -> rowsBytes = bytes);
    }

    /**
     * Gets a set of binary rows to be upserted.
     *
     * @return Binary rows.
     */
    public Set<BinaryRow> getRows() {
        if (rows == null && rowsBytes != null) {
            rows = new HashSet<>();

            CommandUtils.readRows(rowsBytes, rows::add);
        }

        return rows;
    }
}
