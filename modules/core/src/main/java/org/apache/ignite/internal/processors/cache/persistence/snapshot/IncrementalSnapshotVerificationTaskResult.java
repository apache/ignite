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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Represents single job result for {@link IncrementalSnapshotVerificationTask}. */
class IncrementalSnapshotVerificationTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Transaction hashes collection. */
    private Map<Object, TransactionsHashRecord> txHashRes;

    /** Partition hashes collection. */
    private Map<PartitionKeyV2, PartitionHashRecordV2> partHashRes;

    /** Partial committed transactions' collection. */
    private Collection<GridCacheVersion> partialCommittedTxs;

    /** Occurred exceptions. */
    private Collection<Exception> exceptions;

    /** */
    public IncrementalSnapshotVerificationTaskResult() {
        // No-op.
    }

    /** */
    IncrementalSnapshotVerificationTaskResult(
        Map<Object, TransactionsHashRecord> txHashRes,
        Map<PartitionKeyV2, PartitionHashRecordV2> partHashRes,
        Collection<GridCacheVersion> partialCommittedTxs,
        Collection<Exception> exceptions
    ) {
        this.txHashRes = txHashRes;
        this.partHashRes = partHashRes;
        this.partialCommittedTxs = partialCommittedTxs;
        this.exceptions = exceptions;
    }

    /** */
    public Map<PartitionKeyV2, PartitionHashRecordV2> partHashRes() {
        return partHashRes;
    }

    /** */
    public Map<Object, TransactionsHashRecord> txHashRes() {
        return txHashRes;
    }

    /** */
    public Collection<GridCacheVersion> partialCommittedTxs() {
        return partialCommittedTxs;
    }

    /** */
    public Collection<Exception> exceptions() {
        return exceptions;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, txHashRes);
        U.writeMap(out, partHashRes);
        U.writeCollection(out, partialCommittedTxs);
        U.writeCollection(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        txHashRes = U.readMap(in);
        partHashRes = U.readMap(in);
        partialCommittedTxs = U.readCollection(in);
        exceptions = U.readCollection(in);
    }
}
