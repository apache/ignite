/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.datagrid.store.hibernate

import org.apache.ignite.cache.store.{CacheStore, CacheStoreAdapter, CacheStoreSession}
import org.apache.ignite.lang.IgniteBiInClosure
import org.apache.ignite.resources.CacheStoreSessionResource
import org.apache.ignite.scalar.examples.datagrid.store._
import org.apache.ignite.transactions.Transaction

import org.hibernate.cfg.Configuration
import org.hibernate.{FlushMode, HibernateException, Session}
import org.jetbrains.annotations.Nullable

import javax.cache.Cache
import javax.cache.integration.{CacheLoaderException, CacheWriterException}
import java.lang.{Long => JavaLong}
import java.util.UUID

/**
 * Example of [[CacheStore]] implementation that uses Hibernate
 * and deals with maps [[UUID]] to [[Person]].
 */
class ScalarCacheHibernatePersonStore extends CacheStoreAdapter[JavaLong, Person]{
    /** Default hibernate configuration resource path. */
    private val DFLT_HIBERNATE_CFG = "/org/apache/ignite/scalar/examples/datagrid/store/hibernate/hibernate.cfg.xml"

    /** Session attribute name. */
    private val ATTR_SES = "HIBERNATE_STORE_SESSION"

    /** Session factory. */
    private val sesFactory = new Configuration().configure(DFLT_HIBERNATE_CFG).buildSessionFactory

    /** Auto-injected store session. */
    @CacheStoreSessionResource private var ses: CacheStoreSession = null

    def load(key: JavaLong): Person = {
        val tx = transaction

        println(">>> Store load [key=" + key + ", xid=" + (if (tx == null) null else tx.xid) + ']')

        val ses = session(tx)

        try {
            ses.get(classOf[Person], key).asInstanceOf[Person]
        }
        catch {
            case e: HibernateException =>
                rollback(ses, tx)

                throw new CacheLoaderException("Failed to load value from cache store with key: " + key, e)
        }
        finally {
            end(ses, tx)
        }
    }

    def write(entry: Cache.Entry[_ <: JavaLong, _ <: Person]) {
        val tx = transaction

        val key = entry.getKey

        val value = entry.getValue

        println(">>> Store put [key=" + key + ", val=" + value + ", xid=" + (if (tx == null) null else tx.xid) + ']')

        if (value == null)
            delete(key)
        else {
            val ses = session(tx)

            try {
                ses.saveOrUpdate(value)
            }
            catch {
                case e: HibernateException =>
                    rollback(ses, tx)

                    throw new CacheWriterException("Failed to put value to cache store [key=" + key + ", val" + value + "]", e)
            }
            finally {
                end(ses, tx)
            }
        }
    }

    @SuppressWarnings(Array("JpaQueryApiInspection")) def delete(key: Object) {
        val tx = transaction

        println(">>> Store remove [key=" + key + ", xid=" + (if (tx == null) null else tx.xid) + ']')

        val ses = session(tx)

        try {
            ses.createQuery("delete " + classOf[Person].getSimpleName + " where key = :key")
                .setParameter("key", key).setFlushMode(FlushMode.ALWAYS).executeUpdate
        }
        catch {
            case e: HibernateException =>
                rollback(ses, tx)

                throw new CacheWriterException("Failed to remove value from cache store with key: " + key, e)
        }
        finally {
            end(ses, tx)
        }
    }

    override def loadCache(clo: IgniteBiInClosure[JavaLong, Person], args: AnyRef*) {
        if (args == null || args.length == 0 || args(0) == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.")

        val entryCnt = args(0).asInstanceOf[Integer]

        val ses = session(null)

        try {
            var cnt = 0

            val res = ses.createCriteria(classOf[Person]).list

            if (res != null) {
                val iter = res.iterator

                while (cnt < entryCnt && iter.hasNext) {
                    val person = iter.next.asInstanceOf[Person]

                    clo.apply(person.getId, person)

                    cnt += 1
                }
            }
            println(">>> Loaded " + cnt + " values into cache.")
        }
        catch {
            case e: HibernateException =>
                throw new CacheLoaderException("Failed to load values from cache store.", e)
        }
        finally {
            end(ses, null)
        }
    }

    /**
     * Rolls back hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private def rollback(ses: Session, tx: Transaction) {
        if (tx == null) {
            val hTx = ses.getTransaction

            if (hTx != null && hTx.isActive)
                hTx.rollback()
        }
    }

    /**
     * Ends hibernate session.
     *
     * @param ses Hibernate session.
     * @param tx Cache ongoing transaction.
     */
    private def end(ses: Session, @Nullable tx: Transaction) {
        if (tx == null) {
            val hTx = ses.getTransaction

            if (hTx != null && hTx.isActive)
                hTx.commit()

            ses.close
        }
    }

    override def sessionEnd(commit: Boolean) {
        val tx = ses.transaction

        val props = ses.properties

        val session: Session = props.remove(ATTR_SES)

        if (session != null) {
            val hTx = session.getTransaction

            if (hTx != null) {
                try {
                    if (commit) {
                        session.flush()

                        hTx.commit()
                    }
                    else
                        hTx.rollback()

                    println("Transaction ended [xid=" + tx.xid + ", commit=" + commit + ']')
                }
                catch {
                    case e: HibernateException =>
                        throw new CacheWriterException("Failed to end transaction [xid=" + tx.xid + ", commit=" + commit + ']', e)
                }
                finally {
                    session.close
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
    private def session(@Nullable tx: Transaction): Session = {
        var hbSes: Session = null

        if (tx != null) {
            val props = ses.properties[String, Session]()

            hbSes = props.get(ATTR_SES)

            if (hbSes == null) {
                hbSes = sesFactory.openSession

                hbSes.beginTransaction

                props.put(ATTR_SES, hbSes)

                println("Hibernate session open [ses=" + hbSes + ", tx=" + tx.xid + "]")
            }
        }
        else {
            hbSes = sesFactory.openSession

            hbSes.beginTransaction
        }

        hbSes
    }

    /**
     * @return Current transaction.
     */
    @Nullable private def transaction: Transaction = {
        if (ses != null)
            ses.transaction
        else
            null
    }
}
