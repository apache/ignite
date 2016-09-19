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

package org.apache.ignite.internal.processors.platform.entityframework;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheExtension;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.HashSet;
import java.util.Set;

/**
 * EntityFramework cache extension.
 */
public class PlatformDotNetEntityFrameworkCacheExtension implements PlatformCacheExtension {
    /** Extension ID. */
    private static final int EXT_ID = 1;

    /** Operation: increment entity set versions. */
    private static final int OP_INVALIDATE_SETS = 1;

    /** {@inheritDoc} */
    @Override public int id() {
        return EXT_ID;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public long processInOutStreamLong(PlatformCache target, int type, BinaryRawReaderEx reader,
        PlatformMemory mem) throws IgniteCheckedException {
        switch (type) {
            case OP_INVALIDATE_SETS: {
                int cnt = reader.readInt();
                Set<String> entitySetNames = new HashSet(cnt);

                for (int i = 0; i < cnt; i++)
                    entitySetNames.add(reader.readString());

                target.rawCache().invokeAll(entitySetNames,
                    new PlatformDotNetEntityFrameworkIncreaseVersionProcessor());

                // TODO: Broadcast cleanup from here.

                return target.writeResult(mem, null);
            }
        }

        throw new IgniteCheckedException("Unsupported operation type: " + type);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetEntityFrameworkCacheExtension.class, this);
    }
}
