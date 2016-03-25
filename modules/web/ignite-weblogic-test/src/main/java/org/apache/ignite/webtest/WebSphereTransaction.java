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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class WebSphereTransaction implements Transaction {
//    /** */
//    private static final Class<?> onePhaseXAResourceClass;
//
//    static {
//        Class cl;
//
//        try {
//            cl = Class.forName("com.ibm.tx.jta.OnePhaseXAResource");
//        }
//        catch (final ClassNotFoundException e) {
////            throw new IgniteException(e);
//            e.printStackTrace();
//
//            cl = null;
//        }
//
//        onePhaseXAResourceClass = cl;
//    }

    /** */
    private Transaction tx;

    /**
     * @param tx Transaction.
     */
    public WebSphereTransaction(Transaction tx) {
        assert tx != null;
        
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
        SecurityException, IllegalStateException, SystemException {
        System.out.println(">>>>> DEBUG_INFO tx commit : " + this);

        tx.commit();
    }

    /** {@inheritDoc} */
    @Override public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
        System.out.println(">>>>> DEBUG_INFO tx delistRes: " + this);

        return tx.delistResource(xaRes, flag);
    }

    /** {@inheritDoc} */
    @Override public boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException,
        SystemException {
//        System.out.println(">>>>> DEBUG_INFO tx enlist: " + this);

        U.dumpStack(">>>>> DEBUG_INFO tx enlist: " + this);

        return tx.enlistResource(xaRes);
    }

    /** {@inheritDoc} */
    @Override public int getStatus() throws SystemException {
        System.out.println(">>>>> DEBUG_INFO tx getStatus: " + this);

        return tx.getStatus();
    }

    /** {@inheritDoc} */
    @Override public void registerSynchronization(Synchronization sync) throws RollbackException,
        IllegalStateException, SystemException {
        System.out.println(">>>>> DEBUG_INFO tx registSeync: " + this);

        tx.registerSynchronization(sync);
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws IllegalStateException, SystemException {
        System.out.println(">>>>> DEBUG_INFO tx rollback: " + this);

        tx.rollback();
    }

    /** {@inheritDoc} */
    @Override public void setRollbackOnly() throws IllegalStateException, SystemException {
        System.out.println(">>>>> DEBUG_INFO tx setRollback: " + this);

        tx.setRollbackOnly();
    }
}

