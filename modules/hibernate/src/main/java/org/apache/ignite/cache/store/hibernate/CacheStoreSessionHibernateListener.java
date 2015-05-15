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

import org.apache.ignite.cache.store.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.resources.*;
import org.hibernate.*;

import javax.cache.integration.*;
import java.util.*;

/**
 * Cache store session listener based on Hibernate session.
 */
public class CacheStoreSessionHibernateListener implements CacheStoreSessionListener {
    /** Session key for JDBC connection. */
    public static final String HIBERNATE_SES_KEY = "__hibernate_ses_";

    /** Hibernate session factory. */
    private SessionFactory sesFactory;

    /** Store session. */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

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

                    if (tx != null)
                        tx.commit();
                }
                else if (tx != null)
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
