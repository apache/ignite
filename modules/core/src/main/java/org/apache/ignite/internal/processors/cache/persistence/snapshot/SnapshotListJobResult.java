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

    /** */
    @Order(0)
    String[] snpNames;

    /** */
    @Order(1)
    long[] sizes;

    /** */
    @Order(2)
    long[] creationTimes;

    /** */
    @Order(3)
    int[] incCnt;

    /** */
    @Order(4)
    long[] incSizes;

    /** Default constructor for serialization purposes. */
    public SnapshotListJobResult() {
        // No-op.
    }

    /** */
    public SnapshotListJobResult(String[] snpNames, long[] sizes, long[] creationTimes, int[] incCnt, long[] incSizes) {
        this.snpNames = snpNames;
        this.sizes = sizes;
        this.creationTimes = creationTimes;
        this.incCnt = incCnt;
        this.incSizes = incSizes;
    }

    /** */
    public String[] snapshotNames() {
        return snpNames;
    }

    /** */
    public long[] sizes() {
        return sizes;
    }

    /** */
    public long[] creationTimes() {
        return creationTimes;
    }

    /** */
    public int[] incrementalsCount() {
        return incCnt;
    }

    /** */
    public long[] incrementalsSizes() {
        return incSizes;
    }
}
