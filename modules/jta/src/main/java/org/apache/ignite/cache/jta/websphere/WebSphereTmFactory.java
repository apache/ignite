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

package org.apache.ignite.cache.jta.websphere;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import javax.cache.configuration.Factory;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import org.apache.ignite.IgniteException;

/**
 * Implementation of Transaction Manager factory that should used within
 * WebSphere Application Server ("full profile" / "traditional" WS AS).
 * <p>
 * Notes:
 * <ul>
 * <li>
 *     {@link WebSphereLibertyTmFactory} should be used within WebSphere Liberty.
 * </li>
 * <li>
 *      The implementation has been tested with WebSphere Application Server 8.5.5.
 * </li>
 * </ul>
 * <h2 class="header">Java Configuration</h2>
 * <pre name="code" class="java">
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * TransactionConfiguration txCfg = new TransactionConfiguration();
 *
 * txCfg.setTxManagerFactory(new WebSphereTmFactory());
 *
 * cfg.setTransactionConfiguration(new txCfg);
 * </pre>
 * <h2 class="header">Spring Configuration</h2>
 * <pre name="code" class="xml">
 * &lt;bean id="ignite.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *         ...
 *         &lt;property name="transactionConfiguration"&gt;
 *             &lt;bean class="org.apache.ignite.cache.jta.websphere.WebSphereTmFactory"/&gt;
 *         &lt;/property&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>*
 */
public class WebSphereTmFactory implements Factory<TransactionManager> {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private static final Class<?> onePhaseXAResourceCls;

    static {
        try {
            onePhaseXAResourceCls = Class.forName("com.ibm.tx.jta.OnePhaseXAResource");
        }
        catch (ClassNotFoundException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public TransactionManager create() {
        try {
            Class clazz = Class.forName("com.ibm.tx.jta.impl.TranManagerSet");

            Method m = clazz.getMethod("instance", (Class[])null);

            TransactionManager tranMgr = (TransactionManager)m.invoke(null, (Object[])null);

            return new WebSphereTransactionManager(tranMgr);
        }
        catch (SecurityException | ClassNotFoundException | IllegalArgumentException | NoSuchMethodException
            | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /**
     *
     */
    private static class WebSphereTransactionManager implements TransactionManager {
        /** */
        private TransactionManager mgr;

        /**
         * @param mgr Transaction Manager.
         */
        WebSphereTransactionManager(TransactionManager mgr) {
            this.mgr = mgr;
        }

        /** {@inheritDoc} */
        @Override public void begin() throws NotSupportedException, SystemException {
            mgr.begin();
        }

        /** {@inheritDoc} */
        @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException {
            mgr.commit();
        }

        /** {@inheritDoc} */
        @Override public int getStatus() throws SystemException {
            return mgr.getStatus();
        }

        /** {@inheritDoc} */
        @Override public Transaction getTransaction() throws SystemException {
            Transaction tx = mgr.getTransaction();

            if (tx == null)
                return null;

            return new WebSphereTransaction(tx);
        }

        /** {@inheritDoc} */
        @Override public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException,
            SystemException {
            mgr.resume(tobj);
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IllegalStateException, SecurityException, SystemException {
            mgr.rollback();
        }

        /** {@inheritDoc} */
        @Override public void setRollbackOnly() throws IllegalStateException, SystemException {
            mgr.setRollbackOnly();
        }

        /** {@inheritDoc} */
        @Override public void setTransactionTimeout(int seconds) throws SystemException {
            mgr.setTransactionTimeout(seconds);
        }

        /** {@inheritDoc} */
        @Override public Transaction suspend() throws SystemException {
            return mgr.suspend();
        }
    }

    /**
     *
     */
    private static class WebSphereTransaction implements Transaction {
        /** */
        private final Transaction tx;

        /**
         * @param tx Transaction.
         */
        WebSphereTransaction(Transaction tx) {
            assert tx != null;

            this.tx = tx;
        }

        /** {@inheritDoc} */
        @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException {
            tx.commit();
        }

        /** {@inheritDoc} */
        @Override public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException,
            SystemException {
            return tx.delistResource(xaRes, flag);
        }

        /** {@inheritDoc} */
        @Override public boolean enlistResource(final XAResource xaRes) throws RollbackException, IllegalStateException,
            SystemException {
            if (xaRes == null)
                return false;

//            final XAResource res = new IgniteOnePhaseXAResource(xaRes);

            Object ibmProxy = Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[] {onePhaseXAResourceCls},
                new InvocationHandler() {
                    @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                        return mtd.invoke(xaRes, args);
                    }
                });

            return tx.enlistResource((XAResource)ibmProxy);
        }

        /** {@inheritDoc} */
        @Override public int getStatus() throws SystemException {
            return tx.getStatus();
        }

        /** {@inheritDoc} */
        @Override public void registerSynchronization(Synchronization sync) throws RollbackException,
            IllegalStateException, SystemException {
            tx.registerSynchronization(sync);
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IllegalStateException, SystemException {
            tx.rollback();
        }

        /** {@inheritDoc} */
        @Override public void setRollbackOnly() throws IllegalStateException, SystemException {
            tx.setRollbackOnly();
        }
    }
}
