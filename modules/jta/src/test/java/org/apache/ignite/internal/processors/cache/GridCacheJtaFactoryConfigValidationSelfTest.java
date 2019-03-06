/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
