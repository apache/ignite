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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Carries latest version of metadata to client as a response for {@link MetadataRequestMessage}.
 */
public class MetadataResponseMessage implements Message {
    /** */
    @Order(0)
    private int typeId;

    /** */
    @Order(value = 1, method = "metadataVersionInfo")
    private BinaryMetadataVersionInfo metaVerInfo;

    /** */
    @Order(2)
    private boolean metadataFound;

    /** */
    @Order(3)
    private boolean error;

    /** */
    public MetadataResponseMessage() {
        // No-op.
    }

    /**
     * @param typeId Type id.
     */
    MetadataResponseMessage(int typeId) {
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 81;
    }

    /**
     * @return Binary metadata version info.
     */
    public BinaryMetadataVersionInfo metadataVersionInfo() {
        return metaVerInfo;
    }

    /**
     * @param metaVerInfo Binary metadata version info.
     */
    public void metadataVersionInfo(BinaryMetadataVersionInfo metaVerInfo) {
        this.metaVerInfo = metaVerInfo;

        metadataFound = metaVerInfo != null;
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

    /**
     * @return {@code true} if metadata was found on server node replied with the response.
     */
    public boolean metadataFound() {
        return metadataFound;
    }

    /**
     * @return {@code true} if any exception happened during preparing response.
     */
    public boolean error() {
        return error;
    }

    /**
     * @param error {@code true} if any exception happened during preparing response.
     */
    public void error(boolean error) {
        this.error = error;
    }

    /**
     * @param metadataFound {@code true} if metadata was found on server node replied with the response.
     */
    public void metadataFound(boolean metadataFound) {
        this.metadataFound = metadataFound;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataResponseMessage.class, this);
    }
}
