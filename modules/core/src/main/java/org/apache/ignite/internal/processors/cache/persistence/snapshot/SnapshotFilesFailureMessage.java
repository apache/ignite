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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Message indicating a failure occurred during processing snapshot files request.
 */
public class SnapshotFilesFailureMessage extends AbstractSnapshotMessage {
    /** Snapshot response message type (value is {@code 179}). */
    public static final short TYPE_CODE = 179;

    /** Exception message which is occurred during snapshot request processing. */
    @Order(value = 1, method = "errorMessage")
    private String errMsg;

    /**
     * Empty constructor.
     */
    public SnapshotFilesFailureMessage() {
        // No-op.
    }

    /**
     * @param reqId Request id to which response related to.
     * @param errMsg Response error message.
     */
    public SnapshotFilesFailureMessage(String reqId, String errMsg) {
        super(reqId);

        this.errMsg = errMsg;
    }

    /**
     * @return Response error message.
     */
    public String errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Response error message.
     */
    public void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotFilesFailureMessage.class, this, super.toString());
    }
}
