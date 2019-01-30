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
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.jta.jndi.CacheJndiTmFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheJndiTmFactorySelfTest extends GridCommonAbstractTest {
    /** */
    private static final String TM_JNDI_NAME = "java:/comp/env/tm/testtm1";

    /** */
    private static final String TM_JNDI_NAME2 = "java:/comp/env/tm/testtm2";

    /** */
    private static final String NOT_TM_JNDI_NAME = "java:/comp/env/tm/wrongClass";

    /** */
    private String initCtxFactoryBackup;

    /** */
    private String urlPkgPrefixesBackup;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        initCtxFactoryBackup = System.getProperty(Context.INITIAL_CONTEXT_FACTORY);
        urlPkgPrefixesBackup = System.getProperty(Context.URL_PKG_PREFIXES);

        // Create initial context
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");

        InitialContext ic = new InitialContext();

        ic.createSubcontext("java:");
        ic.createSubcontext("java:/comp");
        ic.createSubcontext("java:/comp/env");
        ic.createSubcontext("java:/comp/env/tm");

        ic.bind(TM_JNDI_NAME, new TestTransactionManager());
        ic.bind(TM_JNDI_NAME2, new TestTransactionManager2());
        ic.bind(NOT_TM_JNDI_NAME, 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (initCtxFactoryBackup != null)
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY, initCtxFactoryBackup);

        if (urlPkgPrefixesBackup != null)
            System.setProperty(Context.URL_PKG_PREFIXES, urlPkgPrefixesBackup);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFactory() throws Exception {
        CacheJndiTmFactory f = new CacheJndiTmFactory("wrongJndiName", NOT_TM_JNDI_NAME, TM_JNDI_NAME2, TM_JNDI_NAME);

        TransactionManager mgr = f.create();

        assertNotNull(mgr);

        assertTrue("Mgr: " + mgr, mgr instanceof TestTransactionManager2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFactoryException() throws Exception {
        final CacheJndiTmFactory f = new CacheJndiTmFactory("wrongJndiName", NOT_TM_JNDI_NAME, "wrongJndiName2");

        GridTestUtils.assertThrows(log, new Callable<TransactionManager>() {
            @Override public TransactionManager call() throws Exception {
                return f.create();
            }
        }, IgniteException.class, "Failed to lookup TM by");
    }

    /**
     *
     */
    public static class TestTransactionManager implements TransactionManager {
        /** {@inheritDoc} */
        @Override public void begin() throws NotSupportedException, SystemException {
        }

        /** {@inheritDoc} */
        @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException {
        }

        /** {@inheritDoc} */
        @Override public int getStatus() throws SystemException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public Transaction getTransaction() throws SystemException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException,
            SystemException {
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IllegalStateException, SecurityException, SystemException {
        }

        /** {@inheritDoc} */
        @Override public void setRollbackOnly() throws IllegalStateException, SystemException {
        }

        /** {@inheritDoc} */
        @Override public void setTransactionTimeout(int seconds) throws SystemException {
        }

        /** {@inheritDoc} */
        @Override public Transaction suspend() throws SystemException {
            return null;
        }
    }

    /**
     *
     */
    public static class TestTransactionManager2 extends TestTransactionManager{
    }
}
