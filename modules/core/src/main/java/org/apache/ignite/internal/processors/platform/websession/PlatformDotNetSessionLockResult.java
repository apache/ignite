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

import java.sql.Timestamp;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Result of the {@link PlatformDotNetSessionLockProcessor} execution.
 */
public class PlatformDotNetSessionLockResult implements Binarylizable {
    /** Success flag. */
    private boolean success;

    /** Data. */
    private PlatformDotNetSessionData data;

    /** Lock time. */
    private Timestamp lockTime;

    /** Lock id. */
    private long lockId;

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param data Session data.
     * @param lockTime Lock time.
     */
    public PlatformDotNetSessionLockResult(boolean success, PlatformDotNetSessionData data, Timestamp lockTime,
        long lockId) {
        assert success ^ (data == null);

        this.success = success;
        this.data = data;
        this.lockTime = lockTime;
        this.lockId = lockId;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter raw = writer.rawWriter();

        writeBinary(raw);
    }

    /**
     * Writes to a binary writer.
     *
     * @param writer Binary writer.
     */
    public void writeBinary(BinaryRawWriter writer) {
        writer.writeBoolean(success);

        if (success)
            data.writeBinary(writer);

        writer.writeTimestamp(lockTime);
        writer.writeLong(lockId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader raw = reader.rawReader();

        success = raw.readBoolean();

        if (success) {
            data = new PlatformDotNetSessionData();

            data.readBinary(raw);
        }

        lockTime = raw.readTimestamp();
        lockId = raw.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetSessionLockResult.class, this);
    }
}
