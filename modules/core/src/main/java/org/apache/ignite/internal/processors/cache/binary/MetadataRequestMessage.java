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
package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * As {@link DiscoveryCustomMessage} messages are delivered to client nodes asynchronously
 * it is possible that server nodes are allowed to send to clients some BinaryObjects clients don't have metadata for.
 *
 * When client detects obsolete metadata (by checking if current version of metadata has schemaId)
 * it requests up-to-date metadata using communication SPI.
 *
 * API to make a request is provided by {@link BinaryMetadataTransport#requestUpToDateMetadata(int)} method.
 */
public class MetadataRequestMessage implements Message {
    /** */
    @Order(0)
    private int typeId;

    /**
     * Default constructor.
     */
    public MetadataRequestMessage() {
        //No-op.
    }

    /**
     * @param typeId Type ID.
     */
    MetadataRequestMessage(int typeId) {
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 80;
    }

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @param typeId Type ID.
     */
    public void typeId(int typeId) {
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataRequestMessage.class, this);
    }
}
