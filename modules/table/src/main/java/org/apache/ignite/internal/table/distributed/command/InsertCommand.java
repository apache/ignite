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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * The command inserts a row.
 */
public class InsertCommand implements WriteCommand {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(GetCommand.class);

    /** Row. */
    private transient BinaryRow row;

    /*
     * Row bytes.
     * It is a temporary solution, before network have not implement correct serialization BinaryRow.
     * TODO: Remove the field after.
     */
    private byte[] rowBytes;

    /**
     * @param row Row.
     */
    public InsertCommand(@NotNull BinaryRow row) {
        assert row != null;

        this.row = row;

        rowToBytes(row, bytes -> rowBytes = bytes);
    }

    /**
     * Writes a row to byte array.
     *
     * @param row Row.
     * @param consumer Byte array consumer.
     */
    private void rowToBytes(BinaryRow row, Consumer<byte[]> consumer) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            row.writeTo(baos);

            baos.flush();

            consumer.accept(baos.toByteArray());
        }
        catch (IOException e) {
            LOG.error("Could not write row to stream [row=" + row + ']', e);

            consumer.accept(null);
        }
    }

    /**
     * Gets a data row.
     *
     * @return Data row.
     */
    public BinaryRow getRow() {
        if (row == null)
            row = new ByteBufferRow(rowBytes);

        return row;
    }
}
