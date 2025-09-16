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
 * Is sent from coordinator node to client to deliver the results of the user authentication.
 */
public class UserAuthenticateResponseMessage implements Message {
    /** Request ID. */
    @Order(0)
    private IgniteUuid id;

    /** Error message. */
    @Order(value = 1, method = "errorMessage")
    private String errMsg;

    /**
     *
     */
    public UserAuthenticateResponseMessage() {
        // No-op.
    }

    /**
     * @param id Request ID.
     * @param errMsg error message
     */
    public UserAuthenticateResponseMessage(IgniteUuid id, String errMsg) {
        this.id = id;
        this.errMsg = errMsg;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return errMsg == null;
    }

    /**
     * @return Error message.
     */
    public String errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg New error message.
     */
    public void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @return Request ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @param id New request ID.
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 132;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserAuthenticateResponseMessage.class, this);
    }
}
