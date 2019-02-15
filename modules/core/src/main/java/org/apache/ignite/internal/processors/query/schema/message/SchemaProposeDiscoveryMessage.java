/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    @Override public boolean stopProcess() {
        return false;
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
        if (!hasError())
            this.err = err;
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
