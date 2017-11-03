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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for transaction configuration.
 */
public class VisorTransactionConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default cache concurrency. */
    private TransactionConcurrency dfltConcurrency;

    /** Default transaction isolation. */
    private TransactionIsolation dfltIsolation;

    /** Default transaction timeout. */
    private long dfltTimeout;

    /** Pessimistic tx log linger. */
    private int pessimisticTxLogLinger;

    /** Pessimistic tx log size. */
    private int pessimisticTxLogSize;

    /** Transaction manager factory. */
    private String txMgrFactory;

    /**
     * Default constructor.
     */
    public VisorTransactionConfiguration() {
        // No-op.
    }

    /**
     * Whether to use JTA {@code javax.transaction.Synchronization}
     * instead of {@code javax.transaction.xa.XAResource}.
     */
    private boolean useJtaSync;

    /**
     * Create data transfer object for transaction configuration.
     *
     * @param cfg Transaction configuration.
     */
    public VisorTransactionConfiguration(TransactionConfiguration cfg) {
        dfltConcurrency = cfg.getDefaultTxConcurrency();
        dfltIsolation = cfg.getDefaultTxIsolation();
        dfltTimeout = cfg.getDefaultTxTimeout();
        pessimisticTxLogLinger = cfg.getPessimisticTxLogLinger();
        pessimisticTxLogSize = cfg.getPessimisticTxLogSize();
        txMgrFactory = compactClass(cfg.getTxManagerFactory());
        useJtaSync = cfg.isUseJtaSynchronization();
    }

    /**
     * @return Default cache transaction concurrency.
     */
    public TransactionConcurrency getDefaultTxConcurrency() {
        return dfltConcurrency;
    }

    /**
     * @return Default cache transaction isolation.
     */
    public TransactionIsolation getDefaultTxIsolation() {
        return dfltIsolation;
    }

    /**
     * @return Default transaction timeout.
     */
    public long getDefaultTxTimeout() {
        return dfltTimeout;
    }

    /**
     * @return Pessimistic log cleanup delay in milliseconds.
     */
    public int getPessimisticTxLogLinger() {
        return pessimisticTxLogLinger;
    }

    /**
     * @return Pessimistic transaction log size.
     */
    public int getPessimisticTxLogSize() {
        return pessimisticTxLogSize;
    }

    /**
     * @return Transaction manager factory.
     */
    public String getTxManagerFactory() {
        return txMgrFactory;
    }

    /**
     * @return Whether to use JTA {@code javax.transaction.Synchronization}
     *     instead of {@code javax.transaction.xa.XAResource}.
     */
    public boolean isUseJtaSync() {
        return useJtaSync;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeEnum(out, dfltConcurrency);
        U.writeEnum(out, dfltIsolation);
        out.writeLong(dfltTimeout);
        out.writeInt(pessimisticTxLogLinger);
        out.writeInt(pessimisticTxLogSize);
        U.writeString(out, txMgrFactory);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dfltConcurrency = TransactionConcurrency.fromOrdinal(in.readByte());
        dfltIsolation = TransactionIsolation.fromOrdinal(in.readByte());
        dfltTimeout = in.readLong();
        pessimisticTxLogLinger = in.readInt();
        pessimisticTxLogSize = in.readInt();
        txMgrFactory = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorTransactionConfiguration.class, this);
    }
}
