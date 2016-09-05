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

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.sql.Timestamp;

/**
 * Entry processor that locks web session data.
 */
public class LockEntryProcessor implements CacheEntryProcessor<String, BinaryObject, Object> {
    /** {@inheritDoc} */
    @Override public Object process(MutableEntry<String, BinaryObject> entry, Object... objects)
        throws EntryProcessorException {
        // Arg contains lock info: node id + thread id
        // Return result is either BinarizableSessionStateStoreData (when not locked) or lockAge (when locked)

        if (!entry.exists())
            return null;

        BinaryObject data = entry.getValue();

        assert data != null;

        if (data.field("LockNodeId") != null) {
            // Already locked: return lock time.
            Object lockTime = data.field("LockTime");

            assert lockTime != null;

            return lockTime;
        }

        LockInfo lockInfo = (LockInfo)objects[0];

        // Not locked: lock and return result
        final BinaryObjectBuilder builder = data.toBuilder();

        builder.setField("LockNodeId", lockInfo.getLockNodeId());
        builder.setField("LockId", lockInfo.getLockId());
        builder.setField("LockTime", lockInfo.getLockTime());

        entry.setValue(builder.build());

        return data;
    }
}
