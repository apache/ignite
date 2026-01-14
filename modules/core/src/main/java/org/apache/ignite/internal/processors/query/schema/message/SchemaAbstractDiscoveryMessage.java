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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract discovery message for schema operations.
 */
public abstract class SchemaAbstractDiscoveryMessage implements DiscoveryCustomMessage, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID */
    @Order(0)
    private IgniteUuid id;

    /** Operation. */
    @GridToStringInclude
    protected SchemaAbstractOperation op;

    /**
     * Operation bytes. Serialized reprezentation of schema operation.
     * TODO Should be removed in IGNITE-27559
     */
    @Order(value = 1, method = "operationBytes")
    private byte[] opBytes;

    /** Error message. */
    @Order(value = 2, method = "errorMessage")
    private String errMsg;

    /** Error code. */
    @Order(value = 3, method = "errorCode")
    private int errCode;

    /** Error. */
    protected SchemaOperationException err;

    /**
     * Constructor.
     */
    protected SchemaAbstractDiscoveryMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param op Operation.
     */
    protected SchemaAbstractDiscoveryMessage(SchemaAbstractOperation op) {
        id = IgniteUuid.randomUuid();
        errCode = -1;

        this.op = op;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @param id New iD
     */
    public void id(IgniteUuid id) {
        this.id = id;
    }

    /**
     * @return Operation.
     */
    public SchemaAbstractOperation operation() {
        try {
            return op != null ? op : U.unmarshal(Marshallers.jdk(), opBytes, null);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to unmarshal schema operation", e);
        }
    }

    /**
     * @return Operation bytes.
     */
    public byte[] operationBytes() {
        try {
            return opBytes != null ? opBytes : U.marshal(Marshallers.jdk(), op);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to marshal schema operation", e);
        }
    }

    /**
     * @param opBytes Operation bytes.
     */
    public void operationBytes(byte[] opBytes) {
        this.opBytes = opBytes;
    }

    /**
     * @return Error message.
     */
    public String errorMessage() {
        return errMsg;
    }

    /**
     * @param errMsg Error message.
     */
    public void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @return Error code.
     */
    public int errorCode() {
        return errCode;
    }

    /**
     * @param errCode Error code.
     */
    public void errorCode(int errCode) {
        this.errCode = errCode;
    }

    /**
     * Set error.
     *
     * @param err Error.
     */
    public void onError(SchemaOperationException err) {
        if (!hasError()) {
            this.err = err;

            errMsg = err.getMessage();
            errCode = err.code();
        }
    }

    /**
     * @return {@code True} if error was reported during init.
     */
    public boolean hasError() {
        return err != null || errMsg != null || errCode > -1;
    }

    /**
     * @return Error message (if any).
     */
    @Nullable public SchemaOperationException error() {
        return !hasError() ? null :
            err != null ? err : new SchemaOperationException(errMsg, errCode);
    }

    /**
     * @return Whether request must be propagated to exchange thread.
     */
    public abstract boolean exchange();

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaAbstractDiscoveryMessage.class, this);
    }
}
