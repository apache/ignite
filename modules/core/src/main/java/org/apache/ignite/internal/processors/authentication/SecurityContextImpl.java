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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;

/** Represents {@link SecurityContext} implementation that ignores any security permission checks. */
public class SecurityContextImpl implements SecurityContext, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    SecuritySubjectImpl subj;

    /** Empty constructor for serialization purposes. */
    public SecurityContextImpl() {
        // No-op.
    }

    /** */
    public SecurityContextImpl(UUID id, String login, SecuritySubjectType type, InetSocketAddress addr) {
        subj = new SecuritySubjectImpl(id, login, type, addr);
    }

    /** Creates {@link Message} of {@code context}. */
    public static SecurityContextImpl message(SecurityContext ctx) {
        assert ctx instanceof SecurityContextImpl;

        return (SecurityContextImpl)ctx;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject subject() {
        return subj;
    }
}
