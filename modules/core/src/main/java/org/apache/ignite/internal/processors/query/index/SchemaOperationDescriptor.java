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

package org.apache.ignite.internal.processors.query.index;

import org.apache.ignite.internal.processors.query.index.message.SchemaAcceptDiscoveryMessage;
import org.apache.ignite.internal.processors.query.index.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.index.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.processors.query.index.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.UUID;

/**
 * Schema operation descriptor.
 */
public class SchemaOperationDescriptor implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Propose message. */
    private final SchemaProposeDiscoveryMessage msgPropose;

    /** Accept message. */
    private SchemaAcceptDiscoveryMessage msgAccept;

    /** Finish message. */
    private SchemaFinishDiscoveryMessage msgFinish;

    /**
     * Constructor.
     *
     * @param msgPropose Propose message.
     */
    public SchemaOperationDescriptor(SchemaProposeDiscoveryMessage msgPropose) {
        this.msgPropose = msgPropose;
    }

    /**
     * Copying constructor.
     *
     * @param other Other instance.
     */
    public SchemaOperationDescriptor(SchemaOperationDescriptor other) {
        this.msgPropose = other.msgPropose;
        this.msgAccept = other.msgAccept;
        this.msgFinish = other.msgFinish;
    }

    /**
     * @return Operation ID.
     */
    public UUID id() {
        return operation().id();
    }

    /**
     * @return Operation.
     */
    public SchemaAbstractOperation operation() {
        return msgPropose.operation();
    }

    /**
     * @return Cache deployment ID.
     */
    public IgniteUuid cacheDeploymentId() {
        return msgPropose.deploymentId();
    }

    /**
     * @return Space.
     */
    public String space() {
        return operation().space();
    }

    /**
     * @return Propose message.
     */
    public SchemaProposeDiscoveryMessage messagePropose() {
        return msgPropose;
    }

    /**
     * @return Accept message.
     */
    @Nullable public SchemaAcceptDiscoveryMessage messageAccept() {
        return msgAccept;
    }

    /**
     * @param msgAccept Accept message.
     */
    public void messageAccept(SchemaAcceptDiscoveryMessage msgAccept) {
        this.msgAccept = msgAccept;
    }

    /**
     * @return Finish message.
     */
    @Nullable public SchemaFinishDiscoveryMessage messageFinish() {
        return msgFinish;
    }

    /**
     * @param msgFinish Finish message.
     */
    public void messageFinish(SchemaFinishDiscoveryMessage msgFinish) {
        this.msgFinish = msgFinish;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaOperationDescriptor.class, this);
    }
}
