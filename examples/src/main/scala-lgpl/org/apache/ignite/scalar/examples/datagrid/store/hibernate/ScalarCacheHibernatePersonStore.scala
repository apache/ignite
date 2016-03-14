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

package org.apache.ignite.scalar.examples.datagrid.store.hibernate

import java.lang.{Long => JavaLong}
import java.util.UUID
import javax.cache.Cache
import javax.cache.integration.{CacheLoaderException, CacheWriterException}

import org.apache.ignite.cache.store.{CacheStore, CacheStoreAdapter, CacheStoreSession}
import org.apache.ignite.lang.IgniteBiInClosure
import org.apache.ignite.resources.CacheStoreSessionResource
import org.apache.ignite.scalar.examples.model.Person
import org.hibernate.{HibernateException, Session}

import scala.collection.JavaConversions._

/**
 * Example of [[CacheStore]] implementation that uses Hibernate
 * and deals with maps [[UUID]] to [[Person]].
 */
class ScalarCacheHibernatePersonStore extends CacheStoreAdapter[JavaLong, Person]{
    /** Auto-injected store session. */
    @CacheStoreSessionResource private var ses: CacheStoreSession = null

    /** @inheritdoc */
    def load(key: JavaLong): Person = {
        println(">>> Store load [key=" + key + ']')

        val hibSes: Session = ses.attachment()
        
        try {
            hibSes.get(classOf[Person], key).asInstanceOf[Person]
        }
        catch {
            case e: HibernateException => 
                throw new CacheLoaderException("Failed to load value from cache store [key=" + key + ']', e)
        }
    }

    /** @inheritdoc */
    def write(entry: Cache.Entry[_ <: JavaLong, _ <: Person]) {
        val key = entry.getKey
        val value = entry.getValue
        
        println(">>> Store write [key=" + key + ", val=" + value + ']')
        
        val hibSes: Session = ses.attachment()

        try {
            hibSes.saveOrUpdate(value)
        }
        catch {
            case e: HibernateException => 
                throw new CacheWriterException("Failed to put value to cache store [key=" + key + ", val" + value + "]", e)
        }
    }

    /** @inheritdoc */
    @SuppressWarnings(Array("JpaQueryApiInspection")) def delete(key: AnyRef) {
        println(">>> Store delete [key=" + key + ']')

        val hibSes: Session = ses.attachment()

        try {
            hibSes.createQuery("delete " + classOf[Person].getSimpleName + " where key = :key").setParameter("key", key).executeUpdate()
        }
        catch {
            case e: HibernateException =>
                throw new CacheWriterException("Failed to remove value from cache store [key=" + key + ']', e)
        }
    }

    /** @inheritdoc */
    override def loadCache(clo: IgniteBiInClosure[JavaLong, Person], args: AnyRef*) {
        if (args == null || args.isEmpty || args(0) == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.")

        val entryCnt = args(0).asInstanceOf[Integer]

        val hibSes: Session = ses.attachment()

        try {
            var cnt = 0

            val list = hibSes.createCriteria(classOf[Person]).setMaxResults(entryCnt).list()

            if (list != null) {
                list.foreach(obj => {
                    val person = obj.asInstanceOf[Person]

                    clo.apply(person.getId, person)

                    cnt += 1
                })
            }
            println(">>> Loaded " + cnt + " values into cache.")
        }
        catch {
            case e: HibernateException =>
                throw new CacheLoaderException("Failed to load values from cache store.", e)
        }
    }
}
