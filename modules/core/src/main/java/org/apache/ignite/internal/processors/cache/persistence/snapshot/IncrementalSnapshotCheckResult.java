/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/** */
class IncrementalSnapshotCheckResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Transaction hashes collection. */
    private Map<Object, TransactionsHashRecord> txHashRes;

    /**
     * Partition hashes collection. Value is a hash of data entries {@link DataEntry} from WAL segments included
     * into the incremental snapshot.
     */
    private Map<PartitionKey, PartitionHashRecord> partHashRes;

    /** Partially committed transactions' collection. */
    private Collection<GridCacheVersion> partiallyCommittedTxs;

    /** Occurred exceptions. */
    private Collection<Exception> exceptions;

    /** */
    public IncrementalSnapshotCheckResult() {
        // No-op.
    }

    /** */
    IncrementalSnapshotCheckResult(
        Map<Object, TransactionsHashRecord> txHashRes,
        Map<PartitionKey, PartitionHashRecord> partHashRes,
        Collection<GridCacheVersion> partiallyCommittedTxs,
        Collection<Exception> exceptions
    ) {
        this.txHashRes = txHashRes;
        this.partHashRes = partHashRes;
        this.partiallyCommittedTxs = partiallyCommittedTxs;
        this.exceptions = exceptions;
    }

    /** */
    public Map<PartitionKey, PartitionHashRecord> partHashRes() {
        return partHashRes;
    }

    /** */
    public Map<Object, TransactionsHashRecord> txHashRes() {
        return txHashRes;
    }

    /** */
    public Collection<GridCacheVersion> partiallyCommittedTxs() {
        return partiallyCommittedTxs;
    }

    /** */
    public Collection<Exception> exceptions() {
        return exceptions;
    }
}
