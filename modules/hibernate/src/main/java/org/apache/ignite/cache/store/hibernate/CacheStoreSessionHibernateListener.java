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

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;
import org.hibernate.*;
import org.hibernate.cfg.*;

import javax.cache.integration.*;
import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Cache store session listener based on Hibernate session.
 */
public class CacheStoreSessionHibernateListener implements CacheStoreSessionListener, LifecycleAware {
    /** Session key for JDBC connection. */
    public static final String HIBERNATE_SES_KEY = "__hibernate_ses_";

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
     *
     * @param sesFactory Session factory.
     */
    public void setSessionFactory(SessionFactory sesFactory) {
        A.notNull(sesFactory, "sesFactory");

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
        if (sesFactory == null && F.isEmpty(hibernateCfgPath))
            throw new IgniteException("Either session factory or Hibernate configuration file is required by " +
                getClass().getSimpleName() + '.');

        if (!F.isEmpty(hibernateCfgPath)) {
            if (sesFactory == null) {
                try {
                    URL url = new URL(hibernateCfgPath);

                    sesFactory = new Configuration().configure(url).buildSessionFactory();
                }
                catch (MalformedURLException ignored) {
                    // No-op.
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
            else
                U.warn(log, "Hibernate configuration file configured in " + getClass().getSimpleName() +
                    " will be ignored (session factory is already set).");
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        if (closeSesOnStop && sesFactory != null && !sesFactory.isClosed())
            sesFactory.close();
    }

    /** {@inheritDoc} */
    @Override public void onSessionStart(CacheStoreSession ses) {
        Map<String, Session> props = ses.properties();

        if (!props.containsKey(HIBERNATE_SES_KEY)) {
            try {
                Session hibSes = sesFactory.openSession();

                props.put(HIBERNATE_SES_KEY, hibSes);

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
        Session hibSes = ses.<String, Session>properties().remove(HIBERNATE_SES_KEY);

        if (hibSes != null) {
            try {
                Transaction tx = hibSes.getTransaction();

                if (commit) {
                    hibSes.flush();

                    if (tx.isActive())
                        tx.commit();
                }
                else if (tx.isActive())
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
