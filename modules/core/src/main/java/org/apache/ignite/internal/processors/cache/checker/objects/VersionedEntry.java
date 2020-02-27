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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Representation of cache data row with partition states.
 */
public class VersionedEntry extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Key. */
    private VersionedKey key;

    /** Value. */
    private VersionedValue val;

    /**
     * Default constructor.
     */
    public VersionedEntry() {
        // No-op
    }

    /**
     * @param nodeId Node id.
     * @param key Key.
     * @param ver Version.
     * @param val Value.
     * @param updateCntr Update counter.
     * @param recheckStartTime Recheck start time.
     */
    public VersionedEntry(UUID nodeId, KeyCacheObject key, GridCacheVersion ver, CacheObject val, long updateCntr,
        long recheckStartTime) {
        this.key = new VersionedKey(nodeId, key, ver);
        this.val = new VersionedValue(val, ver, updateCntr, recheckStartTime);
    }

    /**
     * @return Entry value.
     */
    public CacheObject val() {
        return val.value();
    }

    /**
     * @return Partition update counter for the moment of read from data store.
     */
    public long updateCntr() {
        return val.updateCounter();
    }

    /**
     * @return Recheck start time.
     */
    public long recheckStartTime() {
        return val.recheckStartTime();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        key = (VersionedKey)in.readObject();
        val = (VersionedValue)in.readObject();
    }

    /**
     * @return Key of this entry.
     */
    public KeyCacheObject key() {
        return key.key();
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return key.nodeId();
    }

    /**
     * @return Write version of current entry.
     */
    public GridCacheVersion ver() {
        return key.ver();
    }
}
