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

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.thread.context.OperationContextDispatcher;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * Transfer for {@link SecurityContext}, id of {@link SecuritySubject}.
 *
 * @see OperationContextDispatcher#collectDistributedAttributes()
 */
public class SecurityContextMessage implements Message {
    /** A value of {@link SecuritySubject#id()} */
    @Order(0)
    UUID subjId;

    /** Transient, effective {@link SecurityContext}. */
    private SecurityContext delegate;

    /** Empty constructor for serialization purposes. */
    public SecurityContextMessage() {
        // No-op.
    }

    /** */
    public SecurityContextMessage(SecurityContext delegate) {
        this.delegate = delegate;
        this.subjId = delegate.subject().id();
    }

    /** */
    public SecurityContext delegate() {
        return delegate;
    }

    /** */
    public void delegate(SecurityContext delegate) {
        this.delegate = delegate;
    }
}
