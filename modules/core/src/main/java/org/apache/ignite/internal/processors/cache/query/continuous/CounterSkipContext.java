/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CounterSkipContext {
    /** */
    private final CacheContinuousQueryEntry entry;

    /** */
    private List<Runnable> procC;

    /**
     * @param part Partition.
     * @param cntr Filtered counter.
     * @param topVer Topology version.
     */
    CounterSkipContext(int part, long cntr, AffinityTopologyVersion topVer) {
        entry = new CacheContinuousQueryEntry(0,
            null,
            null,
            null,
            null,
            false,
            part,
            cntr,
            topVer,
            (byte)0);

        entry.markFiltered();
    }

    /**
     * @return Entry for filtered counter.
     */
    CacheContinuousQueryEntry entry() {
        return entry;
    }

    /**
     * @return Entries
     */
    @Nullable public List<Runnable> processClosures() {
        return procC;
    }

    /**
     * @param c Closure send
     */
    void addProcessClosure(Runnable c) {
        if (procC == null)
            procC = new ArrayList<>();

        procC.add(c);
    }
}
