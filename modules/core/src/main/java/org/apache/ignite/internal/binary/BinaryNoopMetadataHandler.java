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
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;

/**
 * No-op metadata handler.
 */
public class BinaryNoopMetadataHandler implements BinaryMetadataHandler {
    /** Cached singleton instance. */
    private static final BinaryNoopMetadataHandler INSTANCE = new BinaryNoopMetadataHandler();

    /**
     * @return Instance.
     */
    public static BinaryNoopMetadataHandler instance() {
        return INSTANCE;
    }

    /**
     * Private constructor.
     */
    private BinaryNoopMetadataHandler() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addMeta(int typeId, BinaryType meta, boolean failIfUnregistered) throws BinaryObjectException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public BinaryType metadata(int typeId) throws BinaryObjectException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryMetadata metadata0(int typeId) throws BinaryObjectException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public BinaryType metadata(int typeId, int schemaId) throws BinaryObjectException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryType> metadata() throws BinaryObjectException {
        return null;
    }
}
