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
 * Web session lock info.
 */
public class LockInfo implements Binarylizable {
    /** */
    private long lockId;

    /** */
    private UUID lockNodeId;

    /** */
    private Timestamp lockTime;

    /**
     * Gets the lock id.
     */
    public long getLockId() {
        return lockId;
    }

    /**
     * Gets the lock node id.
     */
    public UUID getLockNodeId() {
        return lockNodeId;
    }

    /**
     * Gets the lock time.
     */
    public Timestamp getLockTime() {
        return lockTime;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter raw = writer.rawWriter();

        raw.writeLong(lockId);
        raw.writeUuid(lockNodeId);
        raw.writeTimestamp(lockTime);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader raw = reader.rawReader();

        lockId = raw.readLong();
        lockNodeId = raw.readUuid();
        lockTime = raw.readTimestamp();
    }
}
