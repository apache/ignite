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
    /** User name. */
    @Order(0)
    String name;

    /** User password. */
    @Order(1)
    String passwd;

    /** Request ID. */
    @Order(2)
    IgniteUuid id;

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
        id = IgniteUuid.randomUuid();
    }

    /**
     * @return User name.
     */
    public String name() {
        return name;
    }

    /**
     * @return User password.
     */
    public String password() {
        return passwd;
    }

    /**
     * @return Request ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 131;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserAuthenticateRequestMessage.class, this);
    }
}
