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

package org.apache.ignite.transactions.spring;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientTransactionConfiguration;
import org.apache.ignite.internal.transactions.proxy.ClientTransactionProxyFactory;
import org.apache.ignite.internal.transactions.proxy.TransactionProxyFactory;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * Represents {@link AbstractSpringTransactionManager} implementation that uses thin client to access the cluster and
 * manage transactions. It requires thin client instance to be set before manager use
 * (see {@link #setClientInstance(IgniteClient)}). Note that the same thin client instance must be used to both
 * initialize the transaction manager and perform transactional operations.
 */
public class SpringClientTransactionManager extends AbstractSpringTransactionManager {
    /** No-op Ignite logger. */
    private static final IgniteLogger NOOP_LOG = new NullLogger();

    /** Thin client instance. */
    private IgniteClient cli;

    /** @return Thin client instance that is used for accessing the Ignite cluster. */
    public IgniteClient getClientInstance() {
        return cli;
    }

    /** Sets thin client instance that is used for accessing the Ignite cluster. */
    public void setClientInstance(IgniteClient cli) {
        this.cli = cli;
    }

    /** {@inheritDoc} */
    @Override public void onApplicationEvent(ContextRefreshedEvent evt) {
        if (cli == null) {
            throw new IllegalArgumentException("Failed to obtain thin client instance for accessing the Ignite" +
                " cluster. Check that 'clientInstance' property is set.");
        }

        super.onApplicationEvent(evt);
    }

    /** {@inheritDoc} */
    @Override protected TransactionIsolation defaultTransactionIsolation() {
        return ClientTransactionConfiguration.DFLT_TX_ISOLATION;
    }

    /** {@inheritDoc} */
    @Override protected long defaultTransactionTimeout() {
        return ClientTransactionConfiguration.DFLT_TRANSACTION_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected TransactionConcurrency defaultTransactionConcurrency() {
        return ClientTransactionConfiguration.DFLT_TX_CONCURRENCY;
    }

    /** {@inheritDoc} */
    @Override protected TransactionProxyFactory createTransactionFactory() {
        return new ClientTransactionProxyFactory(cli.transactions());
    }

    /** {@inheritDoc} */
    @Override protected IgniteLogger log() {
        return NOOP_LOG;
    }

    /** {@inheritDoc} */
    @Override protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
        ((IgniteTransactionObject)status.getTransaction()).getTransactionHolder().setRollbackOnly();
    }
}
