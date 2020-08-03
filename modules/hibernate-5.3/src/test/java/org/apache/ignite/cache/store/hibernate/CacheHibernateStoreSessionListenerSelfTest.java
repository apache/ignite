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

import java.io.Serializable;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.CacheStoreSessionListenerAbstractSelfTest;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.resource.transaction.spi.TransactionStatus;

/**
 * Tests for {@link CacheJdbcStoreSessionListener}.
 */
public class CacheHibernateStoreSessionListenerSelfTest extends CacheStoreSessionListenerAbstractSelfTest {
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
                CacheHibernateStoreSessionListener lsnr = new CacheHibernateStoreSessionListener();

                SessionFactory sesFactory = new Configuration().
                    setProperty("hibernate.connection.url", URL).
                    addAnnotatedClass(Table1.class).
                    addAnnotatedClass(Table2.class).
                    buildSessionFactory();

                lsnr.setSessionFactory(sesFactory);

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

            if (write.get()) {
                Session hibSes = ses.attachment();

                switch (ses.cacheName()) {
                    case "cache1":
                        hibSes.save(new Table1(entry.getKey(), entry.getValue()));

                        break;

                    case "cache2":
                        if (fail.get())
                            throw new CacheWriterException("Expected failure.");

                        hibSes.save(new Table2(entry.getKey(), entry.getValue()));

                        break;

                    default:
                        throw new CacheWriterException("Wring cache: " + ses.cacheName());
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            deleteCnt.incrementAndGet();

            checkSession();
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            assertNull(ses.attachment());
        }

        /**
         */
        private void checkSession() {
            Session hibSes = ses.attachment();

            assertNotNull(hibSes);

            assertTrue(hibSes.isOpen());

            Transaction tx = hibSes.getTransaction();

            assertNotNull(tx);

            if (ses.isWithinTransaction())
                assertEquals(TransactionStatus.ACTIVE, tx.getStatus());
            else
                assertFalse("Unexpected status: " + tx.getStatus(), tx.getStatus() == TransactionStatus.ACTIVE);

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
    }

    /**
     */
    @Entity
    @Table(name = "Table1")
    private static class Table1 implements Serializable {
        /** */
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Column(name = "id")
        private Integer id;

        /** */
        @Column(name = "key")
        private int key;

        /** */
        @Column(name = "value")
        private int value;

        /**
         * @param key Key.
         * @param value Value.
         */
        private Table1(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     */
    @Entity
    @Table(name = "Table2")
    private static class Table2 implements Serializable {
        /** */
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Column(name = "id")
        private Integer id;

        /** */
        @Column(name = "key")
        private int key;

        /** */
        @Column(name = "value")
        private int value;

        /**
         * @param key Key.
         * @param value Value.
         */
        private Table2(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }
}
