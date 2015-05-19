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
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.hibernate.*;
import org.hibernate.cfg.Configuration;

import javax.cache.Cache;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.util.*;

/**
 * Tests for {@link CacheStoreSessionJdbcListener}.
 */
public class CacheStoreSessionHibernateListenerSelfTest extends CacheStoreSessionListenerAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Factory<? extends CacheStore<Integer, Integer>> storeFactory() {
        return new Factory<CacheStore<Integer, Integer>>() {
            @Override public CacheStore<Integer, Integer> create() {
                return new Store();
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStoreSessionListener> sessionListenerFactory() {
        return new Factory<CacheStoreSessionListener>() {
            @Override public CacheStoreSessionListener create() {
                CacheStoreSessionHibernateListener lsnr = new CacheStoreSessionHibernateListener();

                Configuration cfg = new Configuration().
                    setProperty("hibernate.connection.url", URL);

                lsnr.setSessionFactory(cfg.buildSessionFactory());

                return lsnr;
            }
        };
    }

    /**
     */
    private static class Store extends CacheStoreAdapter<Integer, Integer> {
        /** */
        private static String SES_CONN_KEY = "ses_conn";

        /** */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, Object... args) {
            loadCacheCnt.incrementAndGet();

            checkSession();
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            loadCnt.incrementAndGet();

            checkSession();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry)
            throws CacheWriterException {
            writeCnt.incrementAndGet();

            checkSession();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            deleteCnt.incrementAndGet();

            checkSession();
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            assertNull(session());
        }

        /**
         */
        private void checkSession() {
            Session hibSes = session();

            assertNotNull(hibSes);

            assertTrue(hibSes.isOpen());

            if (ses.isWithinTransaction())
                assertNotNull(hibSes.getTransaction());
            else
                assertNull(hibSes.getTransaction());

            verifySameInstance(hibSes);
        }

        /**
         * @param hibSes Session.
         */
        private void verifySameInstance(Session hibSes) {
            Map<String, Session> props = ses.properties();

            Session sesConn = props.get(SES_CONN_KEY);

            if (sesConn == null)
                props.put(SES_CONN_KEY, hibSes);
            else {
                assertSame(hibSes, sesConn);

                reuseCnt.incrementAndGet();
            }
        }

        /**
         * @return Connection.
         */
        private Session session() {
            return ses.<String, Session>properties().get(CacheStoreSessionHibernateListener.HIBERNATE_SES_KEY);
        }
    }
}
