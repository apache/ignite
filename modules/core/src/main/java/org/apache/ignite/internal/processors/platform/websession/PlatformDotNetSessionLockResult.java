/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.platform.websession;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.sql.Timestamp;

/**
 * Result of the {@link PlatformDotNetSessionLockProcessor} execution.
 */
@SuppressWarnings({"AssignmentToDateFieldFromParameter", "ReturnOfDateField"})
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
