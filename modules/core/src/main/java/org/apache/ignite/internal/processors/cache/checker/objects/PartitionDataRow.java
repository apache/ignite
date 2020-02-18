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
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Representation of cache data row with partition states.
 */
public class PartitionDataRow extends PartitionKeyVersion {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Value. */
    private CacheObject val;

    /**
     * Partition update counter for the moment of read from data store.
     */
    private long updateCounter;

    /**
     * Recheck start time.
     */
    private long recheckStartTime;

    /**
     * Default constructor.
     */
    public PartitionDataRow() {
    }

    /**
     * @param nodeId Node id.
     * @param key Key.
     * @param ver Version.
     * @param val Value.
     * @param updateCounter Update counter.
     * @param recheckStartTime Recheck start time.
     */
    public PartitionDataRow(UUID nodeId, KeyCacheObject key, GridCacheVersion ver, CacheObject val, long updateCounter,
        long recheckStartTime) {
        super(nodeId, key, ver);
        this.val = val;
        this.updateCounter = updateCounter;
        this.recheckStartTime = recheckStartTime;
    }

    /**
     * @return Entry value.
     */
    public CacheObject getVal() {
        return val;
    }

    /**
     * @return Partition update counter for the moment of read from data store.
     */
    public long getUpdateCounter() {
        return updateCounter;
    }

    /**
     * @return Recheck start time.
     */
    public long getRecheckStartTime() {
        return recheckStartTime;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);
        out.writeObject(val);
        out.writeLong(updateCounter);
        out.writeLong(recheckStartTime);

    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);
        val = (CacheObject)in.readObject();
        updateCounter = in.readLong();
        recheckStartTime = in.readLong();
    }
}
