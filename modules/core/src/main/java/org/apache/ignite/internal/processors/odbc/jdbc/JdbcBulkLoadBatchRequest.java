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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * A JDBC request that sends a batch of a file to the server. Used when handling
 * {@link SqlBulkLoadCommand} command.
 */
public class JdbcBulkLoadBatchRequest extends JdbcRequest {
    /** A sentinel to indicate that {@link #cmd} field was not initialized. */
    public static final int CMD_UNKNOWN = -1;

    /** Next batch comes in this request and there are more batches. */
    public static final int CMD_CONTINUE = 0;

    /**
     * This is the final batch from the client and there was an error on the client side,
     * so terminate with error on the server side as well.
     */
    public static final int CMD_FINISHED_ERROR = 1;

    /**
     * This is the final batch of the file and everything went well on the client side.
     * Server may complete the request.
     */
    public static final int CMD_FINISHED_EOF = 2;

    /** CursorId of the original COPY command request. */
    private long cursorId;

    /** Batch index starting from 0. */
    private int batchIdx;

    /** Command (see CMD_xxx constants above). */
    private int cmd;

    /** Data in this batch. */
    private byte[] data;

    /**
     * Creates the request with uninitialized parameters.
     */
    public JdbcBulkLoadBatchRequest() {
        super(BULK_LOAD_BATCH);

        cursorId = -1;
        batchIdx = -1;
        cmd = CMD_UNKNOWN;
        data = null;
    }

    /**
     * Creates the request with specified parameters and zero-length data.
     * Typically used with {@link #CMD_FINISHED_ERROR} and {@link #CMD_FINISHED_EOF}.
     *
     * @param cursorId The cursor ID from the {@link JdbcBulkLoadAckResult}.
     * @param batchIdx Index of the current batch starting with 0.
     * @param cmd The command ({@link #CMD_CONTINUE}, {@link #CMD_FINISHED_EOF}, or {@link #CMD_FINISHED_ERROR}).
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public JdbcBulkLoadBatchRequest(long cursorId, int batchIdx, int cmd) {
        this(cursorId, batchIdx, cmd, new byte[0]);
    }

    /**
     * Creates the request with the specified parameters.
     *
     * @param cursorId The cursor ID from the {@link JdbcBulkLoadAckResult}.
     * @param batchIdx Index of the current batch starting with 0.
     * @param cmd The command ({@link #CMD_CONTINUE}, {@link #CMD_FINISHED_EOF}, or {@link #CMD_FINISHED_ERROR}).
     * @param data The data block (zero length is acceptable).
     */
    public JdbcBulkLoadBatchRequest(long cursorId, int batchIdx, int cmd, @NotNull byte[] data) {
        super(BULK_LOAD_BATCH);

        this.cursorId = cursorId;
        this.batchIdx = batchIdx;

        assert isCmdValid(cmd) : "Invalid command value: " + cmd;
        this.cmd = cmd;

        this.data = data;
    }

    /**
     * Returns the original cursor ID.
     *
     * @return The original cursor ID.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * Returns the batch index.
     *
     * @return The batch index.
     */
    public long batchIdx() {
        return batchIdx;
    }

    /**
     * Returns the command (see CMD_xxx constants for details).
     *
     * @return The command.
     */
    public int cmd() {
        return cmd;
    }

    /**
     * Returns the data.
     *
     * @return data if data was not supplied
     */
    @NotNull public byte[] data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.writeBinary(writer, protoCtx);

        writer.writeLong(cursorId);
        writer.writeInt(batchIdx);
        writer.writeInt(cmd);
        writer.writeByteArray(data);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        super.readBinary(reader, protoCtx);

        cursorId = reader.readLong();
        batchIdx = reader.readInt();

        int c = reader.readInt();
        if (!isCmdValid(c))
            throw new BinaryObjectException("Invalid command: " + cmd);

        cmd = c;

        data = reader.readByteArray();
        assert data != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBulkLoadBatchRequest.class, this);
    }

    /**
     * Checks if the command value is valid.
     *
     * @param c The command value to check.
     * @return True if valid, false otherwise.
     */
    private static boolean isCmdValid(int c) {
        return c >= CMD_CONTINUE && c <= CMD_FINISHED_EOF;
    }
}
