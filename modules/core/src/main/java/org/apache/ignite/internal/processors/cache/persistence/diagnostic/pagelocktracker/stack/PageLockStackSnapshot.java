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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.DumpProcessor;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageMetaInfoStore;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpProcessor.toStringDump;

/**
 * Page lock stack snapshot.
 */
public class PageLockStackSnapshot implements PageLockDump {
    /** */
    public final String name;

    /** */
    public final long time;

    /** */
    public final int headIdx;

    /** */
    public final PageMetaInfoStore pageIdLocksStack;

    /** */
    public final int nextOp;

    /** */
    public final int nextOpStructureId;

    /** */
    public final long nextOpPageId;

    /**
     *
     */
    public PageLockStackSnapshot(
        String name,
        long time,
        int headIdx,
        PageMetaInfoStore pageIdLocksStack,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        this.name = name;
        this.time = time;
        this.headIdx = headIdx;
        this.pageIdLocksStack = pageIdLocksStack;
        this.nextOp = nextOp;
        this.nextOpStructureId = nextOpStructureId;
        this.nextOpPageId = nextOpPageId;
    }

    /** {@inheritDoc} */
    @Override public void apply(DumpProcessor dumpProcessor) {
        dumpProcessor.processDump(this);
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return time;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toStringDump(this);
    }
}
