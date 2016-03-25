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

/**
 * TODO: Add class description.
 */

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 *
 */
public class WebSphereTransactionManager implements TransactionManager {
    private final ConcurrentMap<Transaction, WebSphereTransaction> txCache = new ConcurrentHashMap<>();

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

        Transaction originalTx = mgr.getTransaction();

        if (originalTx == null)
            return null;

        try {
            WebSphereTransaction wsTxWrapper = new WebSphereTransaction(originalTx);

            WebSphereTransaction prevWsTx = txCache.putIfAbsent(originalTx, wsTxWrapper);

            System.out.println(">>>>> DEBUG_INFO: getting transaction [origTx=" + originalTx + ", wsTx=" + wsTxWrapper + ", prevWsTx=" + prevWsTx);

            if (prevWsTx != null)
                wsTxWrapper = prevWsTx;

            return wsTxWrapper;
        }
        catch (Throwable e) {
            e.printStackTrace();

            throw e;
        }

//            return mgr.getTransaction();
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
//
//        /** {@inheritDoc} */
//        @Override public void begin(int i) throws NotSupportedException, SystemException {
//            mgr.begin(i);
//        }
//
//        /** {@inheritDoc} */
//        @Override public boolean enlist(XAResource rsrc, int i) throws RollbackException,
//            IllegalStateException, SystemException {
//            return mgr.enlist(rsrc, i);
//        }
//
//        /** {@inheritDoc} */
//        @Override public boolean delist(XAResource rsrc, int i) {
//            return mgr.delist(rsrc, i);
//        }
//
//        /** {@inheritDoc} */
//        @Override public int registerResourceInfo(String s, Serializable ser) {
//            return mgr.registerResourceInfo(s, ser);
//        }
//
//        /** {@inheritDoc} */
//        @Override public int registerResourceInfo(String s, Serializable ser, int i) {
//            return mgr.registerResourceInfo(s, ser, i);
//        }
}

