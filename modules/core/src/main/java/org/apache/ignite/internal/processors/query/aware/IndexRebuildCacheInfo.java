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
     * Default constructor for {@link Externalizable}.
     */
    public IndexRebuildCacheInfo() {
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     */
    public IndexRebuildCacheInfo(String cacheName) {
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeLongString(out, cacheName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        cacheName = U.readLongString(in);
    }

    /**
     * Getting cache name.
     *
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexRebuildCacheInfo.class, this);
    }
}
