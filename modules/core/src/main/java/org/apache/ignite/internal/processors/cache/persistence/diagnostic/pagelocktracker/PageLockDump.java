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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpHelper;

/**
 * Base page lock tracker structures dump.
 */
public abstract class PageLockDump {
    /** Page lock log name. */
    public final String name;

    /** Dump creation time. */
    public final long time;

    /** Head position. */
    public final int headIdx;

    /** Next operation. */
    public final int nextOp;

    /** Next data structure. */
    public final int nextOpStructureId;

    /** Next page id. */
    public final long nextOpPageId;

    /** */
    protected PageLockDump(
        String name,
        long time,
        int headIdx,
        int nextOp,
        int nextOpStructureId,
        long nextOpPageId
    ) {
        this.name = name;
        this.time = time;
        this.headIdx = headIdx;
        this.nextOp = nextOp;
        this.nextOpStructureId = nextOpStructureId;
        this.nextOpPageId = nextOpPageId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return ToStringDumpHelper.toStringDump(this);
    }
}
