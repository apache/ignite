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

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.proxy.IgniteCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteClientCacheProxy;
import org.apache.ignite.testframework.GridTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/** Tests Spring Transactions manager implementation that uses thin client to access the Ignite cluster. */
public class IgniteClientSpringTransactionManagerTest extends GridSpringTransactionManagerAbstractTest {
    /** Spring application context. */
    private static AnnotationConfigApplicationContext ctx;

    /** Ignite thin client instance. */
    private static IgniteClient cli;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>(CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();

        ctx = new AnnotationConfigApplicationContext(IgniteClientSpringTransactionManagerApplicationContext.class);
        cli = ctx.getBean(IgniteClient.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        ctx.close();
    }

    /** {@inheritDoc} */
    @Override public IgniteCacheProxy<Integer, String> cache() {
        return new IgniteClientCacheProxy<>(cli.cache(CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override public GridSpringTransactionService service() {
        return ctx.getBean(GridSpringTransactionService.class);
    }

    /** {@inheritDoc} */
    @Override public void testDoSetRollbackOnlyInExistingTransaction() {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                service().putWithNestedError(cache(), 1_000);

                return null;
            },
            UnexpectedRollbackException.class,
            "Transaction rolled back because it has been marked as rollback-only");

        assertEquals(0, cache().size());
    }

    /** */
    @Configuration
    @EnableTransactionManagement
    public static class IgniteClientSpringTransactionManagerApplicationContext {
        /** */
        @Bean
        public GridSpringTransactionService transactionService() {
            return new GridSpringTransactionService();
        }

        /** */
        @Bean
        public IgniteClient igniteClient() {
            return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + DFLT_PORT));
        }

        /** */
        @Bean
        public AbstractSpringTransactionManager transactionManager(IgniteClient cli) {
            IgniteClientSpringTransactionManager mgr = new IgniteClientSpringTransactionManager();

            mgr.setClientInstance(cli);

            return mgr;
        }
    }
}
