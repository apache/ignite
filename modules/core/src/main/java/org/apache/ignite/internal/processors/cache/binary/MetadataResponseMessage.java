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
    /** Response status if any exception happened during preparing response. */
    private static final byte ERROR = -1;

    /** Response status if metadata was not found on server node replied with the response. */
    private static final byte METADATA_NOT_FOUND = 0;

    /** Response status if metadata was found on server node replied with the response. */
    private static final byte METADATA_FOUND = 1;

    /** Type ID. */
    @Order(0)
    private int typeId;

    /** Binary metadata version info. */
    @Order(value = 1, method = "metadataVersionInfo")
    private BinaryMetadataVersionInfo metaVerInfo;

    /** Client response status. */
    @Order(2)
    private byte status = ERROR;

    /** */
    public MetadataResponseMessage() {
        // No-op.
    }

    /**
     * @param typeId Type ID.
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
        if (metaVerInfo != null)
            status = METADATA_FOUND;
        else
            status = METADATA_NOT_FOUND;

        this.metaVerInfo = metaVerInfo;
    }

    /**
     * Marks message if any exception happened during preparing response.
     */
    void markErrorOnRequest() {
        status = ERROR;
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
     * @return {@code true} if metadata was not found on server node replied with the response.
     */
    boolean metadataNotFound() {
        return status == METADATA_NOT_FOUND;
    }

    /**
     * @return Client response status.
     */
    public byte status() {
        return status;
    }

    /**
     * @param status Client response status.
     */
    public void status(byte status) {
        this.status = status;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataResponseMessage.class, this);
    }
}
