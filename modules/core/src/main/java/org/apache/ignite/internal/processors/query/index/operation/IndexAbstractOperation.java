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

package org.apache.ignite.internal.processors.query.index.operation;

import java.io.Serializable;
import java.util.UUID;

/**
 * Abstract operation on index.
 */
public abstract class IndexAbstractOperation implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID of node that initiated this operation. */
    private final UUID cliNodeId;

    /** Operation ID. */
    private final UUID opId;

    /** Space. */
    private final String space;

    /**
     * Constructor.
     *
     * @param cliNodeId Client node ID.
     * @param opId Operation ID.
     * @param space Space.
     */
    public IndexAbstractOperation(UUID cliNodeId, UUID opId, String space) {
        this.cliNodeId = cliNodeId;
        this.opId = opId;
        this.space = space;
    }

    /**
     * @return Client node ID.
     */
    public UUID clientNodeId() {
        return cliNodeId;
    }

    /**
     * @return Operation id.
     */
    public UUID operationId() {
        return opId;
    }

    /**
     * @return Space.
     */
    public String space() {
        return space;
    }

    /**
     * @return Index name.
     */
    public abstract String indexName();
}
