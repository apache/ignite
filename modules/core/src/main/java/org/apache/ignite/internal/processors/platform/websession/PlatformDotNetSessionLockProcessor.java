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

import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * Entry processor that locks web session data.
 */
public class PlatformDotNetSessionLockProcessor implements CacheEntryProcessor<String, PlatformDotNetSessionData, Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lock node id. */
    private UUID lockNodeId;

    /** Lock id. */
    private long lockId;

    /** Lock time. */
    private Timestamp lockTime;

    /**
     * Ctor.
     *
     * @param lockNodeId Lock node id.
     * @param lockId Lock id.
     * @param lockTime Lock time.
     */
    public PlatformDotNetSessionLockProcessor(UUID lockNodeId, long lockId, Timestamp lockTime) {
        this.lockNodeId = lockNodeId;
        this.lockId = lockId;
        this.lockTime = lockTime;
    }

    /** {@inheritDoc} */
    @Override public Object process(MutableEntry<String, PlatformDotNetSessionData> entry, Object... args)
        throws EntryProcessorException {
        if (!entry.exists())
            return null;

        PlatformDotNetSessionData data = entry.getValue();

        assert data != null;

        if (data.isLocked())
            return new PlatformDotNetSessionLockResult(false, null, data.lockTime(), data.lockId());

        // Not locked: lock and return result
        data = data.lock(lockNodeId, lockId, lockTime);

        // Apply.
        entry.setValue(data);

        return new PlatformDotNetSessionLockResult(true, data, null, data.lockId());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetSessionLockProcessor.class, this);
    }
}
