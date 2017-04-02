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

package org.apache.ignite.internal.processors.query.index.message;

import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.query.index.operation.IndexAbstractOperation;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Abstract discovery message for index operations.
 */
public abstract class IndexAbstractDiscoveryMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Operation. */
    @GridToStringInclude
    protected final IndexAbstractOperation op;

    /**
     * Constructor.
     *
     * @param op Operation.
     */
    protected IndexAbstractDiscoveryMessage(IndexAbstractOperation op) {
        this.op = op;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * @return Operation.
     */
    public IndexAbstractOperation operation() {
        return op;
    }

    /**
     * @return Whether request must be propagated to exchange thread.
     */
    public abstract boolean exchange();

    /**
     * @return Deployment ID.
     */
    public abstract IgniteUuid deploymentId();

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexAbstractDiscoveryMessage.class, this);
    }
}
