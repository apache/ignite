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

package org.apache.ignite.scalar.examples.datagrid

import java.io.Serializable

import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.transactions.TransactionConcurrency._
import org.apache.ignite.transactions.TransactionIsolation._
import org.apache.ignite.{IgniteCache, IgniteException}

/**
 * Demonstrates how to use cache transactions.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheTransactionExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val CACHE_NAME = ScalarCacheTransactionExample.getClass.getSimpleName

    scalar(CONFIG) {
        println()
        println(">>> Cache transaction example started.")

        val cache = createCache$[Integer, Account](CACHE_NAME, atomicityMode = CacheAtomicityMode.TRANSACTIONAL)

        try {
            cache.put(1, new Account(1, 100))
            cache.put(2, new Account(1, 200))

            println()
            println(">>> Accounts before deposit: ")
            println(">>> " + cache.get(1))
            println(">>> " + cache.get(2))

            deposit(cache, 1, 100)
            deposit(cache, 2, 200)

            println()
            println(">>> Accounts after transfer: ")
            println(">>> " + cache.get(1))
            println(">>> " + cache.get(2))
            println(">>> Cache transaction example finished.")
        }
        finally {
            cache.close()
        }
    }

    /**
     * Make deposit into specified account.
     *
     * @param acctId Account ID.
     * @param amount Amount to deposit.
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def deposit(cache: IgniteCache[Integer, Account], acctId: Int, amount: Double) {
        val tx = transaction$(PESSIMISTIC, REPEATABLE_READ)

        try {
            val acct = cache.get(acctId)

            assert(acct != null)

            acct.update(amount)

            cache.put(acctId, acct)

            tx.commit()
        }
        finally {
            tx.close()
        }

        println()
        println(">>> Transferred amount: $" + amount)
    }

    /**
     * Account.
     *
     * @param id Account ID.
     * @param balance Balance.
     */
    private class Account(id: Int, var balance: Double) extends Serializable {
        /**
         * Change balance by specified amount.
         *
         * @param amount Amount to add to balance (may be negative).
         */
        private[datagrid] def update(amount: Double) {
            balance += amount
        }

        override def toString: String = {
            "Account [id=" + id + ", balance=$" + balance + ']'
        }
    }
}
