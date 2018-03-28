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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Task for cleaning sing partition.
 */
public class VacuumTask {
    /** */
    private final MvccVersion cleanupVer;

    /** */
    private final GridDhtLocalPartition part;

    /** */
    private final GridFutureAdapter<VacuumMetrics> fut;

    /**
     * @param cleanupVer Cleanup version.
     * @param part Partition to cleanup.
     * @param fut Vacuum future.
     */
    VacuumTask(MvccVersion cleanupVer,
        GridDhtLocalPartition part,
        GridFutureAdapter<VacuumMetrics> fut) {
        this.cleanupVer = cleanupVer;
        this.part = part;
        this.fut = fut;
    }

    /**
     * @return Cleanup version.
     */
    public MvccVersion cleanupVer() {
        return cleanupVer;
    }

    /**
     * @return Partition to cleanup.
     */
    public GridDhtLocalPartition part() {
        return part;
    }

    /**
     * @return Vacuum future.
     */
    public GridFutureAdapter<VacuumMetrics> future() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VacuumTask.class, this);
    }
}