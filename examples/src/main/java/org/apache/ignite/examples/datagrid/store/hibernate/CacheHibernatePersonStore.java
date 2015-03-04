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

package org.apache.ignite.examples.datagrid.store.hibernate;

import org.apache.ignite.cache.store.*;
import org.apache.ignite.examples.datagrid.store.model.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.Transaction;
import org.hibernate.*;
import org.hibernate.cfg.*;
import org.jetbrains.annotations.*;

import javax.cache.integration.*;
import java.util.*;

/**
 * Example of {@link CacheStore} implementation that uses Hibernate
 * and deals with maps {@link UUID} to {@link Person}.
 */
public class CacheHibernatePersonStore extends CacheStoreAdapter<Long, Person> {
    /** Default hibernate configuration resource path. */
    private static final String DFLT_HIBERNATE_CFG = "/org/apache/ignite/examples/datagrid/store/hibernate/hibernate.cfg.xml";

    /** Session attribute name. */
    private static final String ATTR_SES = "HIBERNATE_STORE_SESSION";

    /** Session factory. */
    private SessionFactory sesFactory;

    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /**
     * Default constructor.
     */
    public CacheHibernatePersonStore() {
        sesFactory = new Configuration().configure(DFLT_HIBERNATE_CFG).buildSessionFactory();
    }

    /** {@inheritDoc} */
    @Override public Person load(Long key) {
        Transaction tx = transaction();

        System.out.println(">>> Store load [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        Session ses = session(tx);

        try {
            return (Person) ses.get(Person.class, key);
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new CacheLoaderException("Failed to load value from cache store with key: " + key, e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(javax.cache.Cache.Entry<? extends Long, ? extends Person> entry) {
        Transaction tx = transaction();

        Long key = entry.getKey();

        Person val = entry.getValue();

        System.out.println(">>> Store put [key=" + key + ", val=" + val + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        if (val == null) {
            delete(key);

            return;
        }

        Session ses = session(tx);

        try {
            ses.saveOrUpdate(val);
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new CacheWriterException("Failed to put value to cache store [key=" + key + ", val" + val + "]", e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"JpaQueryApiInspection"})
    @Override public void delete(Object key) {
        Transaction tx = transaction();

        System.out.println(">>> Store remove [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        Session ses = session(tx);

        try {
            ses.createQuery("delete " + Person.class.getSimpleName() + " where key = :key")
                .setParameter("key", key).setFlushMode(FlushMode.ALWAYS).executeUpdate();
        }
        catch (HibernateException e) {
            rollback(ses, tx);

            throw new CacheWriterException("Failed to remove value from cache store with key: " + key, e);
        }
        finally {
            end(ses, tx);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer)args[0];

        Session ses = session(null);

        try {
            int cnt = 0;

            List res = ses.createCriteria(Person.class).list();

            if (res != null) {
                Iterator iter = res.iterator();

                while (cnt < entryCnt && iter.hasNext()) {
                    Person person = (Person)iter.next();

                    clo.apply(person.getId(), person);

                    cnt++;
                }
            }

            System.out.println(">>> Loaded " + cnt + " values into cache.");
        }
        catch (HibernateException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
        finally {
            end(ses, null);
        }
    }

    /**
     * Rolls back hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private void rollback(Session ses, Transaction tx) {
        // Rollback only if there is no cache transaction,
        // otherwise txEnd() will do all required work.
        if (tx == null) {
            org.hibernate.Transaction hTx = ses.getTransaction();

            if (hTx != null && hTx.isActive())
                hTx.rollback();
        }
    }

    /**
     * Ends hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private void end(Session ses, @Nullable Transaction tx) {
        // Commit only if there is no cache transaction,
        // otherwise txEnd() will do all required work.
        if (tx == null) {
            org.hibernate.Transaction hTx = ses.getTransaction();

            if (hTx != null && hTx.isActive())
                hTx.commit();

            ses.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(boolean commit) {
        CacheStoreSession storeSes = session();

        Transaction tx = storeSes.transaction();

        Map<String, Session> props = storeSes.properties();

        Session ses = props.remove(ATTR_SES);

        if (ses != null) {
            org.hibernate.Transaction hTx = ses.getTransaction();

            if (hTx != null) {
                try {
                    if (commit) {
                        ses.flush();

                        hTx.commit();
                    }
                    else
                        hTx.rollback();

                    System.out.println("Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
                }
                catch (HibernateException e) {
                    throw new CacheWriterException("Failed to end transaction [xid=" + tx.xid() +
                        ", commit=" + commit + ']', e);
                }
                finally {
                    ses.close();
                }
            }
        }
    }

    /**
     * Gets Hibernate session.
     *
     * @param tx Cache transaction.
     * @return Session.
     */
    private Session session(@Nullable Transaction tx) {
        Session ses;

        if (tx != null) {
            Map<String, Session> props = session().properties();

            ses = props.get(ATTR_SES);

            if (ses == null) {
                ses = sesFactory.openSession();

                ses.beginTransaction();

                // Store session in session properties, so it can be accessed
                // for other operations on the same transaction.
                props.put(ATTR_SES, ses);

                System.out.println("Hibernate session open [ses=" + ses + ", tx=" + tx.xid() + "]");
            }
        }
        else {
            ses = sesFactory.openSession();

            ses.beginTransaction();
        }

        return ses;
    }

    /**
     * @return Current transaction.
     */
    @Nullable private Transaction transaction() {
        CacheStoreSession ses = session();

        return ses != null ? ses.transaction() : null;
    }

    /**
     * @return Store session.
     */
    private CacheStoreSession session() {
        return ses;
    }
}
