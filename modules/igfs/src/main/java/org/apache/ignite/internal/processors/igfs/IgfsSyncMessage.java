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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

import java.io.Externalizable;
import java.nio.ByteBuffer;

/**
 * Basic sync message.
 */
public class IgfsSyncMessage extends IgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Coordinator node order. */
    @Order(0)
    long order;

    /** Response flag. */
    @Order(1)
    boolean res;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsSyncMessage() {
        // No-op.
    }

    /**
     * @param order Node order.
     * @param res Response flag.
     */
    public IgfsSyncMessage(long order, boolean res) {
        this.order = order;
        this.res = res;
    }

    /**
     * @return Coordinator node order.
     */
    public long order() {
        return order;
    }

    /**
     * @return {@code True} if response message.
     */
    public boolean response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsSyncMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 71;
    }

}
