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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.transactions.spring.IgniteClientSpringTransactionManager;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/** Represents example of using Ignite Spring Transactions integration with the thin client. */
public class SpringTransactionExample {
    /** Ignite cache name. */
    public static final String ACCOUNT_CACHE_NAME = "example-account-cache";

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) {
        try (
            Ignite ignored = Ignition.start(); // Starts an Ignite cluster consisting of one server node.
            AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()
        ) {
            ctx.register(SpringApplicationConfiguration.class);
            ctx.refresh();

            IgniteClientTransactionalService svc = ctx.getBean(IgniteClientTransactionalService.class);

            svc.createAccount("Bob", 1000);
            svc.createAccount("Alice", 100);
            svc.createAccount("Eve", 0);
            svc.createAccount("Dave", 0);

            doFundTransferWithBroker(svc, "Bob", "Alice", "Eve", "Dave", 1000, 10);

            doFundTransferWithBroker(svc, "Bob", "Alice", "Eve", "Dave", 100, 10);
        }
    }

    /** Delegates funds transfer operation to {@link IgniteClientTransactionalService} and logs the result. */
    private static void doFundTransferWithBroker(
        IgniteClientTransactionalService svc,
        String firstEmitter,
        String secondEmitter,
        String recipient,
        String broker,
        int cash,
        int fee
    ) {
        System.out.println("+--------------Fund transfer operation--------------+");

        try {
            svc.transferFundsWithBroker(firstEmitter, secondEmitter, recipient, broker, cash, fee);

            System.out.println(">>> Operation completed successfully");
        }
        catch (RuntimeException e) {
            System.out.println(">>> Operation was rolled back [error = " + e.getMessage() + ']');
        }

        System.out.println("\n>>> Account statuses:");

        System.out.println(">>> " + firstEmitter + " balance: " + svc.getBalance(firstEmitter));
        System.out.println(">>> " + secondEmitter + " balance: " + svc.getBalance(secondEmitter));
        System.out.println(">>> " + recipient + " balance: " + svc.getBalance(recipient));
        System.out.println(">>> " + broker + " balance: " + svc.getBalance(broker));
        System.out.println("+---------------------------------------------------+");
    }

    /** Spring application configuration. */
    @Configuration
    @EnableTransactionManagement
    public static class SpringApplicationConfiguration {
        /**
         * Ignite thin client instance that will be used to both initialize
         * {@link IgniteClientSpringTransactionManager} and perform transactional cache operations.
         *
         * @return Ignite Client.
         */
        @Bean
        public IgniteClient igniteClient() {
            return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT));
        }

        /**
         * Ignite implementation of the Spring Transactions manager interface.
         *
         * @param cli Ignite client.
         * @return Transaction manager.
         */
        @Bean
        public IgniteClientSpringTransactionManager transactionManager(IgniteClient cli) {
            IgniteClientSpringTransactionManager mgr = new IgniteClientSpringTransactionManager();

            mgr.setClientInstance(cli);
            mgr.setTransactionConcurrency(PESSIMISTIC);

            return mgr;
        }

        /**
         * Service instance that uses declarative transaction management when working with the Ignite cache.
         *
         * @param cli Ignite client.
         * @return Service.
         */
        @Bean
        public IgniteClientTransactionalService transactionalService(IgniteClient cli) {
            IgniteClientTransactionalService svc = new IgniteClientTransactionalService();

            svc.setCache(cli.getOrCreateCache(new ClientCacheConfiguration()
                .setName(ACCOUNT_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL)));

            return svc;
        }
    }
}
