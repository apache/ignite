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

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.yardstick.cache.model.Account;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteAccountTxBenchmark extends IgniteAccountTxAbstractBenchmark {
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Set<Integer> accountIds = new TreeSet<>();

        int accNum = args.batch();

        while (accountIds.size() < accNum)
            accountIds.add(nextRandom(args.range()));

        try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Map<Integer, Account> accounts = (Map)cache.getAll(accountIds);

            if (accounts.size() != accNum)
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

        return true;
    }
}
