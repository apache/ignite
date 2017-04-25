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

    /** Error. */
    private final SchemaOperationException err;

    /** Original propose message. */
    private transient SchemaProposeDiscoveryMessage proposeMsg;

    /**
     * Constructor.
     *
     * @param op Original operation.
     * @param err Error.
     */
    public SchemaFinishDiscoveryMessage(SchemaAbstractOperation op, SchemaOperationException err) {
        super(op);

        this.err = err;
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
     * @return {@code True} if error was reported during init.
     */
    public boolean hasError() {
        return err != null;
    }

    /**
     * @return Error message (if any).
     */
    @Nullable public SchemaOperationException error() {
        return err;
    }

    /**
     * @return Propose message.
     */
    public SchemaProposeDiscoveryMessage proposeMessage() {
        return proposeMsg;
    }

    /**
     * @param proposeMsg Propose message.
     */
    public void proposeMessage(SchemaProposeDiscoveryMessage proposeMsg) {
        this.proposeMsg = proposeMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaFinishDiscoveryMessage.class, this, "parent", super.toString());
    }
}
