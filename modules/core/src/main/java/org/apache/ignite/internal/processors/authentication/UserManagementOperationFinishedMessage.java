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

package org.apache.ignite.internal.processors.authentication;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Message indicating that user operation is finished locally on the node.
 * Server nodes send this message to coordinator when the user operation is finished.
 */
public class UserManagementOperationFinishedMessage implements Message {
    /** Operation Id. */
    @Order(value = 0, method = "operationId")
    private IgniteUuid opId;

    /** Error message. */
    @Order(value = 1, method = "errorMessage")
    private String errorMsg;

    /**
     *
     */
    public UserManagementOperationFinishedMessage() {
        // No-op.
    }

    /**
     * @param opId operation id
     * @param errorMsg error message
     */
    public UserManagementOperationFinishedMessage(IgniteUuid opId, String errorMsg) {
        this.opId = opId;
        this.errorMsg = errorMsg;
    }

    /**
     * @return Operation ID.
     */
    public IgniteUuid operationId() {
        return opId;
    }

    /**
     * @param opId New operation ID.
     */
    public void operationId(IgniteUuid opId) {
        this.opId = opId;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return errorMsg == null;
    }

    /**
     * @return Error message.
     */
    public String errorMessage() {
        return errorMsg;
    }

    /**
     * @param errorMsg New error message.
     */
    public void errorMessage(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 130;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserManagementOperationFinishedMessage.class, this);
    }
}
