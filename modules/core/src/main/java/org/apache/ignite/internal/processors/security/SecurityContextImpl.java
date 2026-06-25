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

package org.apache.ignite.internal.processors.security;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;

/** Represents {@link SecurityContext} implementation that ignores any security permission checks. */
public class SecurityContextImpl implements SecurityContext, Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private SecuritySubject subj;

    /** */
    @Order(0)
    transient UUID subjId;

    /** Empty constructor for serialization purposes. */
    public SecurityContextImpl() {
        // No-op.
    }

    /** */
    public SecurityContextImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
        subj = new SecuritySubjectImpl(id, login, type, addr);
    }

    /** */
    public SecurityContextImpl(UUID id) {
        subjId = id;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject subject() {
        return subj;
    }

    /** Represents {@link SecuritySubject} implementation. */
    private static class SecuritySubjectImpl implements SecuritySubject {
        /** */
        private static final long serialVersionUID = 0L;

        /** Security subject identifier. */
        private final UUID id;

        /** Security subject login.  */
        private final String login;

        /** Security subject type. */
        private final SecuritySubjectType type;

        /** Security subject address. */
        private final InetSocketAddress addr;

        /** */
        public SecuritySubjectImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
            this.id = id;
            this.login = login;
            this.type = type;
            this.addr = addr;
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

        /** {@inheritDoc} */
        @Override public InetSocketAddress address() {
            return addr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SecuritySubjectImpl.class, this);
        }
    }
}
