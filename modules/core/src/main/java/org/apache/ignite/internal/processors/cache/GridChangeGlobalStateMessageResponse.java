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

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public final class GridChangeGlobalStateMessageResponse extends GridCacheMessage {
    /** Request id. */
    @Order(value = 3, method = "requestId")
    private UUID reqId;

    /** Activation error message. */
    @Order(value = 4, method = "errorMessage")
    private ErrorMessage errMsg;

    /**
     * Default constructor.
     */
    public GridChangeGlobalStateMessageResponse() {
         /* No-op. */
    }

    /**
     * Constructor.
     */
    public GridChangeGlobalStateMessageResponse(UUID reqId, @Nullable Throwable err) {
        this.reqId = reqId;

        // Minor optimization.
        if (err != null)
            errMsg = new ErrorMessage(err);
    }

    /**
     * @return Request id.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @param reqId Request id.
     */
    public void requestId(UUID reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Activation error.
     */
    public @Nullable Throwable getError() {
        return errMsg.toThrowable();
    }

    /**
     * @return Error message.
     */
    public ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Error message.
     */
    public void errorMessage(ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -45;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridChangeGlobalStateMessageResponse.class, this, super.toString());
    }
}
