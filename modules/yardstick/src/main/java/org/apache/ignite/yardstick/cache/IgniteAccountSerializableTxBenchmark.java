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

package org.apache.ignite.yardstick.cache;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.yardstick.cache.model.Account;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteAccountSerializableTxBenchmark extends IgniteAccountTxAbstractBenchmark {
    /** */
    private static final int ACCOUNT_NUMBER = 3;

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Set<Integer> accountIds = new HashSet<>();

        while (accountIds.size() < ACCOUNT_NUMBER)
            accountIds.add(nextRandom(args.range()));

        while (true) {
            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                Map<Integer, Account> accounts = (Map)cache.getAll(accountIds);

                if (accounts.size() != ACCOUNT_NUMBER)
                    throw new Exception("Failed to find accounts: " + accountIds);

                Integer fromId = accountIds.iterator().next();

                int fromBalance = accounts.get(fromId).balance();

                for (Integer id : accountIds) {
                    if (id.equals(fromId))
                        continue;

                    Account account = accounts.get(id);

                    if (fromBalance > 0) {
                        fromBalance--;

                        cache.put(id, new Account(account.balance() + 1));
                    }
                }

                cache.put(fromId, new Account(fromBalance));

                tx.commit();
            }
            catch (TransactionOptimisticException e) {
                continue;
            }

            break;
        }

        return true;
    }
}