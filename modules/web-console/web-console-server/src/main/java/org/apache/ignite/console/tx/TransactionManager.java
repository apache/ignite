/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.tx;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.db.NestedTransaction;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.errors.Errors.checkDatabaseNotAvailable;
import static org.apache.ignite.console.errors.Errors.convertToDatabaseNotAvailableException;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Transactions manager.
 */
@Service
public class TransactionManager {
    /** */
    private static final Logger log = LoggerFactory.getLogger(TransactionManager.class);

    /** */
    private final Ignite ignite;

    /** Messages accessor. */
    private final MessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

    /** */
    private final Map<String, Runnable> cachesStarters = new ConcurrentHashMap<>();

    /**
     * @param ignite Ignite.
     */
    @Autowired
    protected TransactionManager(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * @return Current transaction.
     */
    private Transaction tx() {
        return ignite.transactions().tx();
    }

    /**
     * Starts new transaction with the specified concurrency and isolation.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     */
    private Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        Transaction curTx = tx();

        return curTx == null ? ignite.transactions().txStart(concurrency, isolation) : new NestedTransaction(curTx);
    }

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    private Transaction txStart() {
        return txStart(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param name Starter name.
     * @param starter Caches starter.
     */
    public void registerStarter(String name, Runnable starter) {
        cachesStarters.putIfAbsent(name, starter);

        starter.run();
    }

    /**
     * Recreate all caches by executing all registered starters.
     */
    private synchronized void recreateCaches() {
        cachesStarters.forEach((name, starter) -> {
            if (log.isDebugEnabled())
                log.debug("Creating caches for: {}", name);

            starter.run();
        });
    }

    /**
     * @param act Action to execute.
     * @return Action result.
     */
    private <T> T doInTransaction0(Callable<T> act) {
        try (Transaction tx = txStart()) {
            T res = act.call();

            tx.commit();

            return res;
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Execute action in transaction.
     *
     * @param act Action to execute.
     * @return Action result.
     */
    public <T> T doInTransaction(Callable<T> act) {
        // For server nodes no need to recreate caches.
        if (!ignite.configuration().isClientMode())
            return doInTransaction0(act);

        // For client nodes:
        //  1. Try to execute action in transaction.
        //  2. If failed, try to recreate caches.
        //  3. If current transaction was nested then throw IllegalStateException.
        //  4. If caches recreated successfully then try to execute action in transaction.
        try {
            return doInTransaction0(act);
        }
        catch (IgniteException e) {
            if (checkDatabaseNotAvailable(e)) {
                try {
                    recreateCaches();
                }
                catch (IgniteException re) {
                    throw convertToDatabaseNotAvailableException(re, messages.getMessage("err.db-not-available"));
                }

                if (tx() instanceof NestedTransaction)
                    throw new IllegalStateException(messages.getMessage("err.db-lost-connection-during-tx"));

                return doInTransaction0(act);
            }

            throw e;
        }
    }

    /**
     * Execute action in transaction.
     *
     * @param act Action to execute.
     */
    public void doInTransaction(Runnable act) {
        doInTransaction(() -> {
            act.run();

            return null;
        });
    }

    /**
     * Checks if active transaction in progress.
     *
     * @throws IllegalStateException If active transaction was not found.
     */
    public void checkInTransaction() {
        if (tx() == null)
            throw new IllegalStateException(messages.getMessage("err.active-tx-not-found"));
    }
}
