/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.meta;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.task.GridInternal;

/**
 * Represents information about cluster metadata.
 */
@GridInternal
public class MetadataMarshalled extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Marshaled metadata. */
    @Order(value = 0)
    byte[] metaMarshalled;

    /** Metadata. */
    @Order(value = 1)
    BinaryMetadata meta;

    /**
     * Constructor for optimized marshaller.
     */
    public MetadataMarshalled() {
        // No-op.
    }

    /**
     * @param metaMarshalled Marshaled metadata.
     * @param meta Meta.
     */
    public MetadataMarshalled(byte[] metaMarshalled, BinaryMetadata meta) {
        this.metaMarshalled = metaMarshalled;
        this.meta = meta;
    }

    /**
     * @return Cluster binary metadata.
     */
    public BinaryMetadata metadata() {
        return meta;
    }

    /**
     * @return Marshalled metadata.
     */
    public byte[] metadataMarshalled() {
        return metaMarshalled;
    }
}
