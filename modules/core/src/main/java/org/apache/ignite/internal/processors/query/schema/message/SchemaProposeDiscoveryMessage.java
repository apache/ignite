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
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Schema change propose discovery message.
 */
public class SchemaProposeDiscoveryMessage extends SchemaAbstractDiscoveryMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache deployment ID. */
    private IgniteUuid depId;

    /** Error. */
    private SchemaOperationException err;

    /** Whether to perform exchange. */
    private transient boolean exchange;

    /**
     * Constructor.
     *
     * @param op Operation.
     */
    public SchemaProposeDiscoveryMessage(SchemaAbstractOperation op) {
        super(op);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean exchange() {
        return exchange;
    }

    /**
     * @param exchange Whether to perform exchange.
     */
    public void exchange(boolean exchange) {
        this.exchange = exchange;
    }

    /**
     * @return Deployment ID.
     */
    @Nullable public IgniteUuid deploymentId() {
        return depId;
    }

    /**
     * @param depId Deployment ID.
     */
    public void deploymentId(IgniteUuid depId) {
        this.depId = depId;
    }

    /**
     *
     * @return {@code True} if message is initialized.
     */
    public boolean initialized() {
        return deploymentId() != null || hasError();
    }

    /**
     * Set error.
     *
     * @param err Error.
     */
    public void onError(SchemaOperationException err) {
        if (!hasError()) {
            this.err = err;
        }
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
     * @return Schema name.
     */
    public String schemaName() {
        return operation().schemaName();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaProposeDiscoveryMessage.class, this, "parent", super.toString());
    }
}
