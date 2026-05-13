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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

/** */
public class IncrementalSnapshotVerifyResult implements Message, Serializable {
    /** Transaction hashes collection. */
    @Order(0)
    Collection<TransactionsHashRecord> txHashRes;

    /**
     * Partition hashes collection. Value is a hash of data entries {@link DataEntry} from WAL segments included
     * into the incremental snapshot.
     */
    @Order(1)
    Collection<PartitionHashRecord> partHashRes;

    /** Partially committed transactions' collection. */
    @Order(2)
    Collection<GridCacheVersion> partiallyCommittedTxs;

    /** Occurred exceptions. */
    @Order(3)
    Collection<ErrorMessage> exceptions;

    /** Default constructor for {@link MessageFactory}. */
    public IncrementalSnapshotVerifyResult() {
        // No-op.
    }

    /** */
    IncrementalSnapshotVerifyResult(
        Collection<TransactionsHashRecord> txHashRes,
        Collection<PartitionHashRecord> partHashRes,
        Collection<GridCacheVersion> partiallyCommittedTxs,
        Collection<Exception> exceptions
    ) {
        this.txHashRes = txHashRes;
        this.partHashRes = partHashRes;
        this.partiallyCommittedTxs = partiallyCommittedTxs;
        this.exceptions = exceptions == null ? null : F.viewReadOnly(exceptions, ErrorMessage::new);
    }

    /** */
    public Collection<PartitionHashRecord> partHashRes() {
        return partHashRes;
    }

    /** */
    public Collection<TransactionsHashRecord> txHashRes() {
        return txHashRes;
    }

    /** */
    public Collection<GridCacheVersion> partiallyCommittedTxs() {
        return partiallyCommittedTxs;
    }
}
