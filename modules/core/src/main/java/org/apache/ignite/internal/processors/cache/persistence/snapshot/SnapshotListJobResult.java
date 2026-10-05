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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;

/** Accumulated result of {@link SnapshotListTask}. */
public final class SnapshotListJobResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Snapshot names. */
    @Order(0)
    String[] snpName;

    /** Snapshot sizes. */
    @Order(1)
    long[] sz;

    /** Creation times. */
    @Order(2)
    long[] creationTime;

    /** Last modified times. Actual if {@link #incCnt}[n] > 0. */
    @Order(3)
    long[] editTime;

    /** Numbers of related incremental snapshots. */
    @Order(4)
    int[] incCnt;

    /** Total sizes of related incremental snapshots. */
    @Order(5)
    long[] incSize;

    /** Default constructor for serialization purposes. */
    public SnapshotListJobResult() {
        // No-op.
    }

    /** */
    public SnapshotListJobResult(String[] name, long[] sz, long[] createTime, long[] editTime, int[] incCnt, long[] incSize) {
        this.snpName = name;
        this.sz = sz;
        this.creationTime = createTime;
        this.editTime = editTime;
        this.incCnt = incCnt;
        this.incSize = incSize;
    }

    /** */
    public String[] snapshotNames() {
        return snpName;
    }

    /** */
    public long[] sizes() {
        return sz;
    }

    /** */
    public long[] creationTimes() {
        return creationTime;
    }

    /** */
    public long[] editTimes() {
        return editTime;
    }

    /** */
    public int[] incrementalsCount() {
        return incCnt;
    }

    /** */
    public long[] incrementalsSizes() {
        return incSize;
    }
}
