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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/** */
public class IncrementalSnapshotVerifyResult implements Message {
    /** Transaction hashes collection. */
    private Map<Object, TransactionsHashRecord> txHashRes;
    
    /** */
    @Order(0)
    private byte[] txHashResBytes;

    /**
     * Partition hashes collection. Value is a hash of data entries {@link DataEntry} from WAL segments included
     * into the incremental snapshot.
     */
    private Map<PartitionKey, PartitionHashRecord> partHashRes;

    /** */
    @Order(1)
    private byte[] partHashResBytes;
    
    /** Partially committed transactions' collection. */
    @Order(2)
    private Collection<GridCacheVersion> partiallyCommittedTxs;

    /** Occurred exceptions. */
    @Order(3)
    private Collection<ErrorMessage> exceptions;

    /** Default constructor for {@link MessageFactory}. */
    public IncrementalSnapshotVerifyResult() {
        // No-op.
    }

    /** */
    IncrementalSnapshotVerifyResult(
        Map<Object, TransactionsHashRecord> txHashRes,
        Map<PartitionKey, PartitionHashRecord> partHashRes,
        Collection<GridCacheVersion> partiallyCommittedTxs,
        Collection<Exception> exceptions
    ) {
        this.txHashRes = txHashRes;
        this.partHashRes = partHashRes;
        this.partiallyCommittedTxs = partiallyCommittedTxs;
        this.exceptions = exceptions == null ? null : F.viewReadOnly(exceptions, ErrorMessage::new);
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
    public void partiallyCommittedTxs(Collection<GridCacheVersion> partiallyCommittedTxs) {
        this.partiallyCommittedTxs = partiallyCommittedTxs;
    }

    /** */
    public Collection<ErrorMessage> exceptions() {
        return exceptions;
    }

    /** */
    public void exceptions(Collection<ErrorMessage> exceptions) {
        this.exceptions = exceptions;
    }

    /** */
    public byte[] txHashResBytes() {
        if (txHashResBytes != null)
            return txHashResBytes;

        try {
            return txHashResBytes = U.marshal(jdk(), txHashRes);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public void txHashResBytes(byte[] txHashResBytes) {
        if (txHashResBytes == null)
            return;

        try {
            txHashRes = U.unmarshal(jdk(), txHashResBytes, U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public byte[] partHashResBytes() {
        if (partHashResBytes != null)
            return partHashResBytes;

        try {
            return partHashResBytes = U.marshal(jdk(), partHashRes);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public void partHashResBytes(byte[] partHashResBytesBytes) {
        if (partHashResBytesBytes == null)
            return;

        try {
            partHashRes = U.unmarshal(jdk(), partHashResBytesBytes, U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 521;
    }
}
