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

package org.apache.ignite.client.proto.query.event;

import org.apache.ignite.client.proto.query.ClientMessage;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC schemas request.
 */
public class JdbcMetaSchemasRequest implements ClientMessage {
    /** Schema search pattern. */
    private String schemaName;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaSchemasRequest() {
    }

    /**
     * Constructor.
     *
     * @param schemaName Schema search pattern.
     */
    public JdbcMetaSchemasRequest(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Gets schema name sql pattern.
     *
     * @return Schema search pattern.
     */
    public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        ClientMessageUtils.writeStringNullable(packer, schemaName);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        schemaName = ClientMessageUtils.readStringNullable(unpacker);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaSchemasRequest.class, this);
    }
}
