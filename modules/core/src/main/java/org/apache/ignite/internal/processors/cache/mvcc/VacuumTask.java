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
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Task for cleaning sing partition.
 */
public class VacuumTask extends GridFutureAdapter<VacuumMetrics> {
    /** */
    private final MvccSnapshot snapshot;

    /** */
    @GridToStringExclude
    private final GridDhtLocalPartition part;

    /**
     * @param snapshot Snapshot.
     * @param part Partition to cleanup.
     */
    VacuumTask(MvccSnapshot snapshot, GridDhtLocalPartition part) {
        this.snapshot = snapshot;
        this.part = part;
    }

    /**
     * @return Snapshot.
     */
    public MvccSnapshot snapshot() {
        return snapshot;
    }

    /**
     * @return Partition to cleanup.
     */
    public GridDhtLocalPartition part() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VacuumTask.class, this, "partId", part.id());
    }
}