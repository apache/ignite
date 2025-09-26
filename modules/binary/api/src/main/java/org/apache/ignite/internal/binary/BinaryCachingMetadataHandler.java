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

package org.apache.ignite.internal.binary;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.binary.BinaryObjectException;

/**
 * Simple caching metadata handler. Used mainly in communication (TCP, JDBC).
 */
class BinaryCachingMetadataHandler implements BinaryMetadataHandler {
    /** Cached {@code BinaryMetadata}. */
    private final Map<Integer, BinaryMetadata> metas = new ConcurrentHashMap<>();

    /**
     * Create new handler instance.
     *
     * @return New handler.
     */
    static BinaryCachingMetadataHandler create() {
        return new BinaryCachingMetadataHandler();
    }

    /**
     * Private constructor.
     */
    private BinaryCachingMetadataHandler() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addMeta(int typeId, BinaryMetadata newMeta, boolean failIfUnregistered)
        throws BinaryObjectException {
        metas.merge(typeId, newMeta, BinaryUtils::mergeMetadata);
    }

    /** {@inheritDoc} */
    @Override public void addMetaLocally(int typeId, BinaryMetadata meta, boolean failIfUnregistered)
        throws BinaryObjectException {
        addMeta(typeId, meta, failIfUnregistered);
    }

    /** {@inheritDoc} */
    @Override public BinaryMetadata metadata0(int typeId) throws BinaryObjectException {
        return metas.get(typeId);
    }

    /** */
    @Override public BinaryMetadata metadata0(int typeId, int schemaId) throws BinaryObjectException {
        BinaryMetadata meta = metas.get(typeId);

        return meta != null && meta.hasSchema(schemaId) ? meta : null;
    }

    /** */
    @Override public Collection<BinaryMetadata> metadata0() throws BinaryObjectException {
        return metas.values();
    }
}
