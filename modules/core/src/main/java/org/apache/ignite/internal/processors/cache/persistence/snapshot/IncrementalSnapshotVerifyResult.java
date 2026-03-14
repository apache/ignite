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
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/** */
public class IncrementalSnapshotVerifyResult implements MarshallableMessage {
    /** Transaction hashes collection. */
    private Map<Object, TransactionsHashRecord> txHashRes;
    
    /** */
    @Order(0)
    byte[] txHashResBytes;

    /**
     * Partition hashes collection. Value is a hash of data entries {@link DataEntry} from WAL segments included
     * into the incremental snapshot.
     */
    private Map<PartitionKey, PartitionHashRecord> partHashRes;

    /** */
    @Order(1)
    byte[] partHashResBytes;
    
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

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        txHashResBytes = U.marshal(marsh, txHashRes);
        partHashResBytes = U.marshal(marsh, partHashRes);

        try {
            finishUnmarshal(jdk(), U.gridClassLoader());
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("MY ERROR");
            System.out.println("MY txHashRes=" + ToStringBuilder.reflectionToString(txHashRes,
                new MultilineRecursiveToStringStyle()));
            System.out.println("MY partHashRes=" + ToStringBuilder.reflectionToString(partHashRes,
                new MultilineRecursiveToStringStyle()));
            System.out.println("MY txHashRes=" + txHashRes);
            System.out.println("MY txHashRes=" + partHashRes);
            System.out.println("MY txHashRes.size()=" + txHashRes.size());
            System.out.println("MY partHashRes.size()=" + partHashRes.size());

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        txHashRes = U.unmarshal(marsh, txHashResBytes, clsLdr);
        partHashRes = U.unmarshal(marsh, partHashResBytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 524;
    }
}
