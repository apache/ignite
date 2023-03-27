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
import java.util.UUID;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;

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
