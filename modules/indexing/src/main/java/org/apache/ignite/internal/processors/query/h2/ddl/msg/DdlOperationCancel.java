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

package org.apache.ignite.internal.processors.query.h2.ddl.msg;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Message that initiates cancel of a DDL operation due to an irrecoverable error on the peer or on the coordinator,
 * or due to the operation's cancellation by the user.
 * May be sent either by the <b>client</b> or by <b>coordinator.</b>
 */
public class DdlOperationCancel implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private IgniteUuid opId;

    /** */
    private IgniteCheckedException err;

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    public IgniteUuid getOperationId() {
        return opId;
    }

    public void setOperationId(IgniteUuid opId) {
        this.opId = opId;
    }

    /**
     * @return Error that has led to this cancellation, or {@code null} if it's user's cancel.
     */
    public IgniteCheckedException getError() {
        return err;
    }

    /**
     * @param err Error that has led to this cancellation, or {@code null} if it's user's cancel.
     */
    public void setError(IgniteCheckedException err) {
        this.err = err;
    }
}
