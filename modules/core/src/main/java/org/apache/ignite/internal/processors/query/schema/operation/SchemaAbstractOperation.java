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

package org.apache.ignite.internal.processors.query.schema.operation;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Abstract operation on schema.
 */
public abstract class SchemaAbstractOperation implements Serializable, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Operation ID. */
    @Order(0)
    UUID opId;

    /** Cache name. */
    @Order(1)
    String cacheName;

    /** Schema name. */
    @Order(2)
    String schemaName;

    /** */
    protected SchemaAbstractOperation() {}

    /**
     * Constructor.
     *
     * @param opId Operation ID.
     * @param schemaName Schema name.
     */
    protected SchemaAbstractOperation(UUID opId, String cacheName, String schemaName) {
        this.opId = opId;
        this.cacheName = cacheName;
        this.schemaName = schemaName;
    }

    /**
     * @return Operation id.
     */
    public UUID id() {
        return opId;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaAbstractOperation.class, this);
    }
}
