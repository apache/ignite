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

package org.apache.ignite.transactions.spring.examples;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.client.ClientCache;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;
import static org.springframework.transaction.annotation.Isolation.REPEATABLE_READ;

/**
 * Represents Spring Service that uses Ignite thin client to access Ignite cluster and perform cache transactional
 * operations.
 */
@Service
public class IgniteClientTransactionalService {
    /** Ignite cache representation that uses thin client to communicate with Ignite cluster. */
    private ClientCache<String, Account> cache;

    /**
     * The emitters transfer the specified funds to the broker. When both funds are received, they are transferred to
     * the recipient, excluding the fee which the broker keeps for himself. If an error occurs at any step of this
     * operation, it is rolled back.
     *
     * @param firstEmitter First emitter.
     * @param secondEmitter Second emitter.
     * @param recipient Recipient.
     * @param broker Broker.
     * @param funds Funds.
     * @param fee Fee.
     */
    @Transactional(isolation = REPEATABLE_READ)
    public void transferFundsWithBroker(
        String firstEmitter,
        String secondEmitter,
        String recipient,
        String broker,
        int funds,
        int fee
    ) {
        transferFunds(firstEmitter, broker, funds);

        transferFunds(secondEmitter, broker, funds);

        transferFunds(broker, recipient, funds * 2 - fee);
    }

    /**
     * Transfers funds between two accounts that belong to users with the specified names.
     *
     * @param emitter Emitter.
     * @param recipient Recipient.
     * @param funds Funds.
     */
    @Transactional(isolation = REPEATABLE_READ)
    public void transferFunds(String emitter, String recipient, int funds) {
        Account emitterAcc = cache.get(emitter);
        Account recipientAcc = cache.get(recipient);

        if (emitterAcc.balance < funds)
            throw new RuntimeException("Insufficient funds in " + emitter + "'s account");

        emitterAcc.balance -= funds;
        recipientAcc.balance += funds;

        saveAccount(emitterAcc);
        saveAccount(recipientAcc);

        System.out.println(">>> " + emitter + " transfers " + funds + " coins to " + recipient);
    }

    /**
     * Gets current balance of the account with the specified name.
     *
     * @param login Login.
     * @return Balance.
     */
    @Transactional(isolation = READ_COMMITTED)
    public int getBalance(String login) {
        return cache.get(login).balance;
    }

    /**
     * Creates account with the specified user login and balance.
     *
     * @param login Login.
     * @param balance Balance.
     * @return Account.
     */
    public Account createAccount(String login, int balance) {
        Account acc = new Account(login);

        acc.balance = balance;

        cache.put(login, acc);

        return acc;
    }

    /**
     * Sets an Ignite cache representation that uses thin client to communicate with the Ignite cluster.
     *
     * @param cache Cache.
     */
    public void setCache(ClientCache<String, Account> cache) {
        this.cache = cache;
    }

    /**
     * Puts the specified account into the cache.
     *
     * @param acc Account.
     */
    private void saveAccount(Account acc) {
        cache.put(acc.login, acc);
    }

    /** Represents the user account. */
    private static class Account implements Serializable {
        /** Account owner login. */
        private final String login;

        /** Balance. */
        private int balance;

        /**
         * Creates an account with the specified owner name.
         *
         * @param login Login.
         */
        public Account(String login) {
            this.login = login;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            Account acc = (Account)other;

            return balance == acc.balance && Objects.equals(login, acc.login);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(login, balance);
        }
    }
}
