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

package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.util.Set;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class RecoveryContext {
    /** */
    private final WALPointer initPnt;

    /** */
    private final Set<GridCacheVersion> skipTxEntries;

    /**
     *
     */
    public RecoveryContext(WALPointer initPnt, Set<GridCacheVersion> skipTxEntries) {
        this.initPnt = initPnt;
        this.skipTxEntries = skipTxEntries;
    }

    /**
     *
     */
    public WALPointer getInitPnt() {
        return initPnt;
    }

    /**
     *
     */
    public Set<GridCacheVersion> getSkipTxEntries() {
        return skipTxEntries;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "RecoveryContext[" + "initPnt=" + initPnt + ", skipTxEntries=" + skipTxEntries + ']';
    }
}
