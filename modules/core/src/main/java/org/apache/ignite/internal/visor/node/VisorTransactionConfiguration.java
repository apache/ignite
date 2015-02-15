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

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.transactions.*;

import java.io.*;

/**
 * Data transfer object for transaction configuration.
 */
public class VisorTransactionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default cache concurrency. */
    private IgniteTxConcurrency dfltConcurrency;

    /** Default transaction isolation. */
    private IgniteTxIsolation dfltIsolation;

    /** Default transaction timeout. */
    private long dfltTxTimeout;

    /** Pessimistic tx log linger. */
    private int pessimisticTxLogLinger;

    /** Pessimistic tx log size. */
    private int pessimisticTxLogSize;

    /** Default transaction serializable flag. */
    private boolean txSerEnabled;

    /**
     * Create data transfer object for transaction configuration.
     *
     * @param src Transaction configuration.
     * @return Data transfer object.
     */
    public static VisorTransactionConfiguration from(TransactionConfiguration src) {
        VisorTransactionConfiguration cfg = new VisorTransactionConfiguration();

        cfg.defaultTxConcurrency(src.getDefaultTxConcurrency());
        cfg.defaultTxIsolation(src.getDefaultTxIsolation());
        cfg.defaultTxTimeout(src.getDefaultTxTimeout());
        cfg.pessimisticTxLogLinger(src.getPessimisticTxLogLinger());
        cfg.pessimisticTxLogSize(src.getPessimisticTxLogSize());
        cfg.txSerializableEnabled(src.isTxSerializableEnabled());

        return cfg;
    }

    /**
     * @return Default cache transaction concurrency.
     */
    public IgniteTxConcurrency defaultTxConcurrency() {
        return dfltConcurrency;
    }

    /**
     * @param dfltConcurrency Default cache transaction concurrency.
     */
    public void defaultTxConcurrency(IgniteTxConcurrency dfltConcurrency) {
        this.dfltConcurrency = dfltConcurrency;
    }

    /**
     * @return Default cache transaction isolation.
     */
    public IgniteTxIsolation defaultTxIsolation() {
        return dfltIsolation;
    }

    /**
     * @param dfltIsolation Default cache transaction isolation.
     */
    public void defaultTxIsolation(IgniteTxIsolation dfltIsolation) {
        this.dfltIsolation = dfltIsolation;
    }

    /**
     * @return Default transaction timeout.
     */
    public long defaultTxTimeout() {
        return dfltTxTimeout;
    }

    /**
     * @param dfltTxTimeout Default transaction timeout.
     */
    public void defaultTxTimeout(long dfltTxTimeout) {
        this.dfltTxTimeout = dfltTxTimeout;
    }

    /**
     * @return Pessimistic log cleanup delay in milliseconds.
     */
    public int pessimisticTxLogLinger() {
        return pessimisticTxLogLinger;
    }

    /**
     * @param pessimisticTxLogLinger Pessimistic log cleanup delay.
     */
    public void pessimisticTxLogLinger(int pessimisticTxLogLinger) {
        this.pessimisticTxLogLinger = pessimisticTxLogLinger;
    }

    /**
     * @return Pessimistic transaction log size.
     */
    public int getPessimisticTxLogSize() {
        return pessimisticTxLogSize;
    }

    /**
     * @param pessimisticTxLogSize Pessimistic transactions log size.
     */
    public void pessimisticTxLogSize(int pessimisticTxLogSize) {
        this.pessimisticTxLogSize = pessimisticTxLogSize;
    }

    /**
     * @return {@code True} if serializable transactions are enabled, {@code false} otherwise.
     */
    public boolean txSerializableEnabled() {
        return txSerEnabled;
    }

    /**
     * @param txSerEnabled Flag to enable/disable serializable cache transactions.
     */
    public void txSerializableEnabled(boolean txSerEnabled) {
        this.txSerEnabled = txSerEnabled;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorTransactionConfiguration.class, this);
    }
}
