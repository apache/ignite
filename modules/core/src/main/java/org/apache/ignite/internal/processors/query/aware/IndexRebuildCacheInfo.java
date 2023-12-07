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

package org.apache.ignite.internal.processors.query.aware;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Information about the cache for which index rebuilding was started.
 * Designed for MetaStorage.
 */
public class IndexRebuildCacheInfo extends IgniteDataTransferObject {
    /** Serial version UUID. */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /**
     * {@code True} if index.bin recreating, {@code false} otherwise.
     * @see IndexesRebuildTask
     */
    private boolean recreate;

    /**
     * Default constructor for {@link Externalizable}.
     */
    public IndexRebuildCacheInfo() {
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param recreate {@code True} if index.bin recreating, {@code false} otherwise.
     */
    public IndexRebuildCacheInfo(String cacheName, boolean recreate) {
        this.cacheName = cacheName;
        this.recreate = recreate;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeLongString(out, cacheName);
        out.writeBoolean(recreate);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        cacheName = U.readLongString(in);
        recreate = protoVer == V2 && in.readBoolean();
    }

    /**
     * Getting cache name.
     *
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** @return {@code True} if index.bin recreating, {@code false} otherwise. */
    public boolean recreate() {
        return recreate;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexRebuildCacheInfo.class, this);
    }
}
