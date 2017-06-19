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

/**
 * SQL listener query execute result.
 */
public class JdbcBatchExecuteResult extends JdbcResult {
    /** Update counts. */
    private int [] updateCnts;

    /** Batch update error code. */
    private int batchErrCode;

    /** Batch update error message. */
    private String batchErr;

    /**
     * Condtructor.
     */
    public JdbcBatchExecuteResult() {
        super(BATCH_EXEC);
    }

    /**
     * @param updateCnts Update counts for batch.
     * @param errCode Error code.
     * @param errMsg Error message.
     */
    public JdbcBatchExecuteResult(int [] updateCnts, int errCode, String errMsg) {
        super(BATCH_EXEC);

        this.updateCnts = updateCnts;
    }

    /**
     * @return Update count for DML queries.
     */
    public int[] updateCounts() {
        return updateCnts;
    }

    /**
     * @return Batch error code.
     */
    public int batchErrorCode() {
        return batchErrCode;
    }

    /**
     * @return Batch error message.
     */
    public String batchErrorMessage() {
        return batchErr;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeIntArray(updateCnts);
    }


    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        updateCnts = reader.readIntArray();
    }
}
