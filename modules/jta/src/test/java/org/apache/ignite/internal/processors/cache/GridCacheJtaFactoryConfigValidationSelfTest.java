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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import javax.cache.configuration.Factory;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Configuration validation test.
 */
public class GridCacheJtaFactoryConfigValidationSelfTest extends GridCommonAbstractTest {
    /** */
    private Factory factory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxManagerFactory(factory);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(ATOMIC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNullFactory() throws Exception {
        factory = new NullTxFactory();

        Throwable e = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        e.getCause().getMessage().startsWith("Failed to create transaction manager");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWrongTypeFactory() throws Exception {
        factory = new IntegerTxFactory();

        Throwable e = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        e.getCause().getMessage().startsWith("Failed to create transaction manager");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExceptionFactory() throws Exception {
        factory = new ExceptionTxFactory();

        Throwable e = GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(0);

                return null;
            }
        }, IgniteCheckedException.class, null);

        e.getCause().getMessage().startsWith("Failed to create transaction manager");
    }

    /**
     *
     */
    public static class NullTxFactory implements Factory<TransactionManager> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public TransactionManager create() {
            return null;
        }
    }

    /**
     *
     */
    public static class IntegerTxFactory implements Factory<Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Integer create() {
            return 1;
        }
    }

    /**
     *
     */
    public static class ExceptionTxFactory implements Factory {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object create() {
            throw new UnsupportedOperationException();
        }
    }
}
