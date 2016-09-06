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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.sql.Timestamp;
import java.util.UUID;

/**
 * Web session state data.
 */
@SuppressWarnings({"ReturnOfDateField", "AssignmentToDateFieldFromParameter"})
public class SessionStateData implements Binarylizable {
    /** */
    private int timeout;

    /** */
    private UUID lockNodeId;

    /** */
    private long lockId;

    /** */
    private Timestamp lockTime;

    /** */
    private KeyValueDirtyTrackedCollection items;

    /** */
    private byte[] staticObjects;

    /**
     * Gets the lock node id.
     */
    public UUID getLockNodeId() {
        return lockNodeId;
    }

    /**
     * Sets the lock node id.
     *
     */
    public void setLockNodeId(UUID lockNodeId) {
        this.lockNodeId = lockNodeId;
    }

    /**
     * Gets the lock id.
     *
     */
    public long getLockId() {
        return lockId;
    }

    /**
     * Sets the lock id.
     */
    public void setLockId(long lockId) {
        this.lockId = lockId;
    }

    /**
     * Gets the lock time.
     */
    public Timestamp getLockTime() {
        return lockTime;
    }

    /**
     * Sets the lock time.
     */
    public void setLockTime(Timestamp lockTime) {
        this.lockTime = lockTime;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter raw = writer.rawWriter();

        raw.writeInt(timeout);
        raw.writeUuid(lockNodeId);
        raw.writeLong(lockId);
        raw.writeTimestamp(lockTime);
        raw.writeObject(items);
        raw.writeByteArray(staticObjects);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader raw = reader.rawReader();

        timeout = raw.readInt();
        lockNodeId = raw.readUuid();
        lockId = raw.readLong();
        lockTime = raw.readTimestamp();
        items = raw.readObject();
        staticObjects = raw.readByteArray();
    }

    /**
     * Apply changes from another instance.
     *
     * @param other Data.
     */
    public void applyChanges(SessionStateData other) {
        assert other != null;
        assert items != null;

        timeout = other.timeout;
        staticObjects = other.staticObjects;
        items.applyChanges(other.items);
    }
}
