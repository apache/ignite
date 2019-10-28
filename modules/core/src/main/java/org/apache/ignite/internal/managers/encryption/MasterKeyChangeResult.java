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

package org.apache.ignite.internal.managers.encryption;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class MasterKeyChangeResult extends MasterKeyChangeAbstractMessage {
    /**Error that caused this change to be rejected. */
    private IgniteException err;

    /** */
    private final boolean isAck;

    /**
     * @param req Request.
     * @param err Error.
     */
    MasterKeyChangeResult(MasterKeyChangeRequest req, IgniteException err) {
        super(req.requestId(), req.encKeyName(), req.digest());

        this.err = err;
        this.isAck = false;
    }

    /**
     * Constructor for the ack message.
     *
     * @param req Master key change result.
     */
    private MasterKeyChangeResult(MasterKeyChangeResult req) {
        super(req.requestId(), null, null);

        this.err = null;
        this.isAck = true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return isAck || hasError() ? null : new MasterKeyChangeResult(this);
    }

    /** @return Error that caused this change to be rejected. */
    public IgniteException error() {
        return err;
    }

    /** @return {@code True} if has error. */
    public boolean hasError() {
        return err != null;
    }

    /** @return {@code True} if this is ack message. */
    public boolean isAck() {
        return isAck;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MasterKeyChangeResult.class, this);
    }
}
