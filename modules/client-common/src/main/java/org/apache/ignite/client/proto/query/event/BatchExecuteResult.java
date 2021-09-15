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

package org.apache.ignite.client.proto.query.event;

import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC batch execute result.
 */
public class BatchExecuteResult extends Response {
    /** Update counts. */
    private int[] updateCnts;

    /**
     * Constructor.
     */
    public BatchExecuteResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err Error message.
     */
    public BatchExecuteResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param updateCnts Update counts for batch.
     */
    public BatchExecuteResult(int[] updateCnts) {
        Objects.requireNonNull(updateCnts);

        this.updateCnts = updateCnts;

        hasResults = true;
    }

    /**
     * Get the update count for DML queries.
     *
     * @return Update count for DML queries.
     */
    public int[] updateCounts() {
        return updateCnts;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults)
            return;

        packer.packIntArray(updateCnts);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults)
            return;

        updateCnts = unpacker.unpackIntArray();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BatchExecuteResult.class, this);
    }
}
