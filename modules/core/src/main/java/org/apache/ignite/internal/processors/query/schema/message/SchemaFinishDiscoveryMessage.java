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

package org.apache.ignite.internal.processors.query.schema.message;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Schema change finish discovery message.
 */
public class SchemaFinishDiscoveryMessage extends SchemaAbstractDiscoveryMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** No-op flag. */
    @Order(4)
    private boolean nop;

    /**
     * Constructor.
     */
    public SchemaFinishDiscoveryMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param op Original operation.
     * @param err Error.
     * @param nop No-op flag.
     */
    public SchemaFinishDiscoveryMessage(SchemaAbstractOperation op, SchemaOperationException err, boolean nop) {
        super(op);

        this.err = err;
        this.nop = nop;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean exchange() {
        return false;
    }

    /**
     * @return <code>True</code> if message in no-op.
     */
    public boolean nop() {
        return nop;
    }

    /**
     * @param nop No-op flag.
     */
    public void nop(boolean nop) {
        this.nop = nop;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaFinishDiscoveryMessage.class, this, "parent", super.toString());
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 501;
    }
}
