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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * IGFS write blocks message.
 */
public class IgfsBlocksMessage extends IgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    @Order(0)
    IgniteUuid fileId;

    /** Batch id */
    @Order(1)
    long id;

    /** Blocks to store. */
    @GridDirectMap(keyType = IgfsBlockKey.class, valueType = byte[].class)
    @Order(2)
    Map<IgfsBlockKey, byte[]> blocks;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public IgfsBlocksMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param fileId File ID.
     * @param id Message id.
     * @param blocks Blocks to put in cache.
     */
    public IgfsBlocksMessage(IgniteUuid fileId, long id, Map<IgfsBlockKey, byte[]> blocks) {
        this.fileId = fileId;
        this.id = id;
        this.blocks = blocks;
    }

    /**
     * @return File id.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return Batch id.
     */
    public long id() {
        return id;
    }

    /**
     * @return Map of blocks to put in cache.
     */
    public Map<IgfsBlockKey, byte[]> blocks() {
        return blocks;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 66;
    }

}
