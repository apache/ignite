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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/** FIXME SHQ */
public class JdbcBulkLoadFileBatchRequest extends JdbcRequest {

    public enum Command {
        CONTINUE,
        FINISHED_ERROR,
        FINISHED_EOF
    }

    @NotNull private long queryId;
    @NotNull private int batchNum = 0;
    @NotNull private Command cmd;
    @NotNull private byte[] data;

    public JdbcBulkLoadFileBatchRequest() {
        super(BULK_LOAD_BATCH);
    }

    public JdbcBulkLoadFileBatchRequest(long queryId, int num, Command error) {
        this(queryId, num, error, new byte[0]);
    }

    /**
     * @param queryId
     * @param batchNum
     * @param cmd
     * @param data
     */
    public JdbcBulkLoadFileBatchRequest(long queryId, int batchNum, Command cmd, byte[] data) {
        super(BULK_LOAD_BATCH);
        this.queryId = queryId;
        this.batchNum = batchNum;
        this.cmd = cmd;
        this.data = data;
    }

    /**
     * Returns the original query ID.
     *
     * @return The original query ID.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * Returns the batchNum.
     *
     * @return batchNum.
     */
    public long batchNum() {
        return batchNum;
    }

    /**
     * Returns the cmd.
     *
     * @return cmd.
     */
    public Command cmd() {
        return cmd;
    }

    /**
     * Returns the data.
     *
     * @return data or null if data was not supplied
     */
    public byte[] data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeLong(queryId);
        writer.writeInt(batchNum);
        //writer.writeEnum(cmd); FIXME SHQ
        writer.writeInt(cmd.ordinal());
        writer.writeByteArray(data);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        queryId = reader.readLong();
        batchNum = reader.readInt();
        // cmd = reader.readEnum(); FIXME SHQ
        cmd = Command.values()[reader.readInt()];
        data = reader.readByteArray();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcBulkLoadFileBatchRequest.class, this);
    }

}
