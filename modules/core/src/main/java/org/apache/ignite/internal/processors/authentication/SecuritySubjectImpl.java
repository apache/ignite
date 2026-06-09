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

import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.jetbrains.annotations.Nullable;

/**
 * Represents {@link SecuritySubject} implementation.
 */
public class SecuritySubjectImpl implements SecuritySubject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Security subject identifier. */
    @Order(0)
    UUID id;

    /** Security subject login. Is null if {@link SecuritySubjectImpl} is used as {@link Message}. */
    private @Nullable String login;

    /** Security subject type. Is null if {@link SecuritySubjectImpl} is used as {@link Message}. */
    private @Nullable SecuritySubjectType type;

    /** Security subject address. Is null if {@link SecuritySubjectImpl} is used as {@link Message}. */
    private @Nullable InetSocketAddress addr;

    /** Empty constructor for serialization purposes. */
    public SecuritySubjectImpl() {
        // No-op.
    }

    /** */
    public SecuritySubjectImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
        this.id = id;
        this.login = login;
        this.type = type;
        this.addr = addr;
    }

    /** */
    public static SecuritySubjectImpl of(SecuritySubject subj) {
        if (subj instanceof SecuritySubjectImpl)
            return (SecuritySubjectImpl)subj;

        return new SecuritySubjectImpl(subj.id(), subj.login().toString(), subj.type(), subj.address());
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String login() {
        return login;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubjectType type() {
        return type;
    }

    /** @inheritDoc} */
    @Override public InetSocketAddress address() {
        return addr;
    }

    /**{@inheritDoc} */
    @Override public String toString() {
        return S.toString(SecuritySubjectImpl.class, this);
    }
}
