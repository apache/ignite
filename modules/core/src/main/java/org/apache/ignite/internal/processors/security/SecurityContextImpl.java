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
import org.jetbrains.annotations.Nullable;

/** Represents {@link SecurityContext} implementation that ignores any security permission checks. */
public class SecurityContextImpl implements SecurityContext, Message, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Security subject identifier. */
    @Order(0)
    public UUID subjId;

    /** */
    private @Nullable SecuritySubject subj;

    /** Empty constructor for serialization purposes. */
    public SecurityContextImpl() {
        // No-op.
    }

    /** Constructor to be a {@link Message} only. */
    public SecurityContextImpl(UUID subjId) {
        this.subjId = subjId;
    }

    /** */
    public SecurityContextImpl(SecuritySubject subj) {
        this.subjId = subj.id();
        this.subj = subj;
    }

    /** */
    public SecurityContextImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
        subjId = id;
        subj = new SecuritySubjectImpl(login, type, addr);
    }

    /**  */
    public static @Nullable SecurityContextImpl of(@Nullable SecurityContext ctx) {
        if (ctx == null || ctx instanceof SecurityContextImpl)
            return (SecurityContextImpl)ctx;

        return new SecurityContextImpl(ctx.subject());
    }

    /** {@inheritDoc} */
    @Override public @Nullable SecuritySubject subject() {
        return subj;
    }

    /** Represents {@link SecuritySubject} implementation. */
    private class SecuritySubjectImpl implements SecuritySubject {
        /** */
        private static final long serialVersionUID = 0L;

        /** Security subject login.  */
        private final String login;

        /** Security subject type. */
        private final SecuritySubjectType type;

        /** Security subject address. */
        private final InetSocketAddress addr;

        /** */
        private SecuritySubjectImpl(String login, SecuritySubjectType type, InetSocketAddress addr) {
            this.login = login;
            this.type = type;
            this.addr = addr;
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return subjId;
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
