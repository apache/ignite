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

package org.apache.ignite.cache.store.hibernate;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.LoggerResource;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.resource.transaction.spi.TransactionStatus;

/**
 * Hibernate-based cache store session listener.
 * <p>
 * This listener creates a new Hibernate session for each store
 * session. If there is an ongoing cache transaction, a corresponding
 * Hibernate transaction is created as well.
 * <p>
 * The Hibernate session is saved as a store session
 * {@link CacheStoreSession#attachment() attachment}.
 * The listener guarantees that the session will be
 * available for any store operation. If there is an
 * ongoing cache transaction, all operations within this
 * transaction will share a DB transaction.
 * <p>
 * As an example, here is how the {@link CacheStore#write(javax.cache.Cache.Entry)}
 * method can be implemented if {@link CacheHibernateStoreSessionListener}
 * is configured:
 * <pre name="code" class="java">
 * private static class Store extends CacheStoreAdapter&lt;Integer, Integer&gt; {
 *     &#64;CacheStoreSessionResource
 *     private CacheStoreSession ses;
 *
 *     &#64;Override public void write(Cache.Entry&lt;? extends Integer, ? extends Integer&gt; entry) throws CacheWriterException {
 *         // Get Hibernate session from the current store session.
 *         Session hibSes = ses.attachment();
 *
 *         // Persist the value.
 *         hibSes.persist(entry.getValue());
 *     }
 * }
 * </pre>
 * Hibernate session will be automatically created by the listener
 * at the start of the session and closed when it ends.
 * <p>
 * {@link CacheHibernateStoreSessionListener} requires that either
 * {@link #setSessionFactory(SessionFactory)} session factory}
 * or {@link #setHibernateConfigurationPath(String) Hibernate configuration file}
 * is provided. If non of them is set, exception is thrown. Is both are provided,
 * session factory will be used.
 */
public class CacheHibernateStoreSessionListener implements CacheStoreSessionListener, LifecycleAware {
    /** Hibernate session factory. */
    private SessionFactory sesFactory;

    /** Hibernate configuration file path. */
    private String hibernateCfgPath;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Whether to close session on stop. */
    private boolean closeSesOnStop;

    /**
     * Sets Hibernate session factory.
     * <p>
     * Either session factory or configuration file is required.
     * If none is provided, exception will be thrown on startup.
     *
     * @param sesFactory Session factory.
     */
    public void setSessionFactory(SessionFactory sesFactory) {
        this.sesFactory = sesFactory;
    }

    /**
     * Gets Hibernate session factory.
     *
     * @return Session factory.
     */
    public SessionFactory getSessionFactory() {
        return sesFactory;
    }

    /**
     * Sets hibernate configuration path.
     * <p>
     * Either session factory or configuration file is required.
     * If none is provided, exception will be thrown on startup.
     *
     * @param hibernateCfgPath Hibernate configuration path.
     */
    public void setHibernateConfigurationPath(String hibernateCfgPath) {
        this.hibernateCfgPath = hibernateCfgPath;
    }

    /**
     * Gets hibernate configuration path.
     *
     * @return Hibernate configuration path.
     */
    public String getHibernateConfigurationPath() {
        return hibernateCfgPath;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void start() throws IgniteException {
        if (sesFactory == null && F.isEmpty(hibernateCfgPath)) {
            throw new IgniteException("Either session factory or Hibernate configuration file is required by " +
                getClass().getSimpleName() + '.');
        }

        if (!F.isEmpty(hibernateCfgPath)) {
            if (sesFactory == null) {
                try {
                    URL url = new URL(hibernateCfgPath);

                    sesFactory = new Configuration().configure(url).buildSessionFactory();
                }
                catch (MalformedURLException ex) {
                    log.warning("Exception on store listener start", ex);
                }

                if (sesFactory == null) {
                    File cfgFile = new File(hibernateCfgPath);

                    if (cfgFile.exists())
                        sesFactory = new Configuration().configure(cfgFile).buildSessionFactory();
                }

                if (sesFactory == null)
                    sesFactory = new Configuration().configure(hibernateCfgPath).buildSessionFactory();

                if (sesFactory == null)
                    throw new IgniteException("Failed to resolve Hibernate configuration file: " + hibernateCfgPath);

                closeSesOnStop = true;
            }
            else {
                U.warn(log, "Hibernate configuration file configured in " + getClass().getSimpleName() +
                    " will be ignored (session factory is already set).");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (closeSesOnStop && sesFactory != null && !sesFactory.isClosed())
            sesFactory.close();
    }

    /** {@inheritDoc} */
    @Override public void onSessionStart(CacheStoreSession ses) {
        if (ses.attachment() == null) {
            try {
                Session hibSes = sesFactory.openSession();

                ses.attach(hibSes);

                if (ses.isWithinTransaction())
                    hibSes.beginTransaction();
            }
            catch (HibernateException e) {
                throw new CacheWriterException("Failed to start store session [tx=" + ses.transaction() + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
        Session hibSes = ses.attach(null);

        if (hibSes != null) {
            try {
                Transaction tx = hibSes.getTransaction();

                if (commit) {
                    if (hibSes.isDirty())
                        hibSes.flush();

                    if (tx.getStatus() == TransactionStatus.ACTIVE)
                        tx.commit();
                }
                else if (tx.getStatus().canRollback())
                    tx.rollback();
            }
            catch (HibernateException e) {
                throw new CacheWriterException("Failed to end store session [tx=" + ses.transaction() + ']', e);
            }
            finally {
                hibSes.close();
            }
        }
    }
}
