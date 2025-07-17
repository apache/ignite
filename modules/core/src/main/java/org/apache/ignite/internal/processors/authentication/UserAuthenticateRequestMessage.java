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
 * Message is sent from client to coordinator node when a user needs to authorize on client node.
 */
public class UserAuthenticateRequestMessage implements Message {
    /** Request ID. */
    @Order(0)
    private IgniteUuid id = IgniteUuid.randomUuid();

    /** User name. */
    @Order(1)
    private String name;

    /** User password. */
    @Order(2)
    private String passwd;

    /**
     *
     */
    public UserAuthenticateRequestMessage() {
        // No-op.
    }

    /**
     * @param name User name.
     * @param passwd User password.
     */
    public UserAuthenticateRequestMessage(String name, String passwd) {
        this.name = name;
        this.passwd = passwd;
    }

    /**
     * @return User name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name New username.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return User password.
     */
    public String passwd() {
        return passwd;
    }

    /**
     * @param passwd New user password.
     */
    public void passwd(String passwd) {
        this.passwd = passwd;
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
        return 131;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserAuthenticateRequestMessage.class, this);
    }
}
