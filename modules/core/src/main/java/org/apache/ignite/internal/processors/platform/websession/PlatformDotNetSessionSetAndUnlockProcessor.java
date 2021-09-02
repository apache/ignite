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

package org.apache.ignite.internal.processors.platform.websession;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Processor to unlock and optionally update the session.
 */
public class PlatformDotNetSessionSetAndUnlockProcessor implements
    CacheEntryProcessor<String, PlatformDotNetSessionData, Void>, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lock node ID. */
    private UUID lockNodeId;

    /** Lock ID. */
    private long lockId;

    /** Update flag. */
    private boolean update;

    /** Data. */
    private Map<String, byte[]> items;

    /** Whether items collection represents a diff. */
    private boolean isDiff;

    /** Static data. */
    private byte[] staticData;

    /** Timeout. */
    private int timeout;

    /**
     * Constructor for unlock.
     *
     * @param lockNodeId Lock node ID.
     * @param lockId Lock ID.
     */
    public PlatformDotNetSessionSetAndUnlockProcessor(UUID lockNodeId, long lockId) {
        this(lockNodeId, lockId, false, null, false, null, 0);
    }

    /**
     * Constructor for unlock/update.
     *
     * @param data Data.
     */
    public PlatformDotNetSessionSetAndUnlockProcessor(PlatformDotNetSessionData data) {
        this(data.lockNodeId(), data.lockId(), true, data.items(), true, data.staticObjects(), data.timeout());
    }

    /**
     * Constructor.
     *
     * @param lockNodeId Lock node ID.
     * @param lockId Lock ID.
     * @param update Whether to perform update.
     * @param items Items.
     * @param isDiff Whether items is a diff.
     * @param staticData Static data.
     * @param timeout Timeout.
     */
    public PlatformDotNetSessionSetAndUnlockProcessor(UUID lockNodeId, long lockId, boolean update,
        Map<String, byte[]> items, boolean isDiff, byte[] staticData, int timeout) {
        this.lockNodeId = lockNodeId;
        this.lockId = lockId;
        this.update = update;
        this.items = items;
        this.isDiff = isDiff;
        this.staticData = staticData;
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<String, PlatformDotNetSessionData> entry, Object... args)
        throws EntryProcessorException {
        assert entry.exists();

        PlatformDotNetSessionData data = entry.getValue();

        assert data != null;

        // Unlock and update.
        data = update
            ? data.updateAndUnlock(lockNodeId, lockId, items, isDiff, staticData, timeout)
            : data.unlock(lockNodeId, lockId);

        // Apply.
        entry.setValue(data);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter raw = writer.rawWriter();

        raw.writeUuid(lockNodeId);
        raw.writeLong(lockId);
        raw.writeBoolean(update);

        if (update) {
            raw.writeBoolean(isDiff);
            raw.writeByteArray(staticData);
            raw.writeInt(timeout);

            if (items != null) {
                raw.writeInt(items.size());

                for (Map.Entry<String, byte[]> e : items.entrySet()) {
                    raw.writeString(e.getKey());
                    raw.writeByteArray(e.getValue());
                }
            }
            else
                raw.writeInt(-1);
        }
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader raw = reader.rawReader();

        lockNodeId = raw.readUuid();
        lockId = raw.readLong();
        update = raw.readBoolean();

        if (update) {
            isDiff = raw.readBoolean();
            staticData = raw.readByteArray();
            timeout = raw.readInt();

            int cnt = raw.readInt();

            if (cnt >= 0) {
                items = new TreeMap<>();

                for (int i = 0; i < cnt; i++)
                    items.put(raw.readString(), raw.readByteArray());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetSessionSetAndUnlockProcessor.class, this);
    }
}
