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

package org.apache.ignite.webtest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
 *
 */
public class WebSphereTmFactory implements Factory<TransactionManager> {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private static final Class<?> onePhaseXAResourceClass;

    static {
        Class cl;

        try {
             cl = Class.forName("com.ibm.tx.jta.OnePhaseXAResource");
        }
        catch (final ClassNotFoundException e) {
//            throw new IgniteException(e);
            e.printStackTrace();

            cl = null;
        }

        onePhaseXAResourceClass = cl;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public TransactionManager create() {
        System.out.println(">>>>> DEBUG_INFO 1");

        try {
            Class clazz = Class.forName("com.ibm.tx.jta.impl.TranManagerSet");

            Method m = clazz.getMethod("instance", (Class[])null);

            TransactionManager tranMgr = (TransactionManager)m.invoke((Object)null, (Object[])null);

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
            System.out.println(">>>>> DEBUG_INFO: created WS_MGR");

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
            System.out.println(">>>>> DEBUG_INFO: getting transaction");

//            return new WebSphereTransaction(mgr.getTransaction());
            return mgr.getTransaction();
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
//        private static final Class<?> onePhaseXAResourceClass;

        /** */
        private Transaction tx;

//        static {
//            try {
//                onePhaseXAResourceClass = Class.forName("com.ibm.tx.jta.OnePhaseXAResource");
//            }
//            catch (final ClassNotFoundException e) {
//                throw new IgniteException(e);
//            }
//        }

        /**
         * @param tx Transaction.
         */
        WebSphereTransaction(Transaction tx) {
            this.tx = tx;
        }

        /** {@inheritDoc} */
        @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
            SecurityException, IllegalStateException, SystemException {
            tx.commit();
        }

        /** {@inheritDoc} */
        @Override public boolean delistResource(XAResource xaRes,
            int flag) throws IllegalStateException, SystemException {
            return tx.delistResource(xaRes, flag);
        }

        /** {@inheritDoc} */
        @Override public boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException,
            SystemException {
            System.out.println(">>>>> DEBUG_INFO enlisting tx");

//            if (xaRes == null)
//                return false;
//
//            Object o = newProxyInstance(currentThread().getContextClassLoader(),
//                new Class<?>[] {onePhaseXAResourceClass}, new InvocationHandler() {
//                    @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
//                        return mtd.invoke(tx, args);
//                    }
//                });
//
//            return tx.enlistResource((XAResource)o);
//            try {
//                System.out.println("DEBUG_INFO got resource: " + Class.forName("com.ibm.tx.jta.OnePhaseXAResource"));
//            }
//            catch (ClassNotFoundException e) {
//                e.printStackTrace(); // TODO implement.
//
//                System.out.println("DEBUG_INFO got exception in process of gettin resource: " + e);
//            }

            return tx.enlistResource(xaRes);
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
