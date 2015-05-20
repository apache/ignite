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

package org.apache.ignite.scalar.examples.datagrid.store.dummy

import org.apache.ignite.Ignite
import org.apache.ignite.cache.store.{CacheStoreAdapter, CacheStoreSession}
import org.apache.ignite.examples.datagrid.store.Person
import org.apache.ignite.lang.IgniteBiInClosure
import org.apache.ignite.resources.{CacheNameResource, CacheStoreSessionResource, IgniteInstanceResource}
import org.apache.ignite.transactions.Transaction

import org.jetbrains.annotations.Nullable

import javax.cache.Cache
import java.lang.{Long => JavaLong}
import java.util.concurrent.ConcurrentHashMap

/**
 * Dummy cache store implementation.
 */
class ScalarCacheDummyPersonStore extends CacheStoreAdapter[JavaLong, Person] {
    /** Auto-inject ignite instance. */
    @IgniteInstanceResource private var ignite: Ignite = null

    /** Auto-inject cache name. */
    @CacheNameResource private var cacheName: String = null

    /** */
    @CacheStoreSessionResource private var ses: CacheStoreSession = null

    /** Dummy database. */
    private val dummyDB = new ConcurrentHashMap[JavaLong, Person]

    def load(key: JavaLong): Person = {
        val tx = transaction

        println(">>> Store load [key=" + key + ", xid=" + (if (tx == null) null else tx.xid) + ']')

        dummyDB.get(key)
    }

    def write(entry: Cache.Entry[_ <: JavaLong, _ <: Person]) {
        val tx = transaction

        val key = entry.getKey

        val value = entry.getValue

        println(">>> Store put [key=" + key + ", val=" + value + ", xid=" + (if (tx == null) null else tx.xid) + ']')

        dummyDB.put(key, value)
    }

    def delete(key: AnyRef) {
        val tx = transaction

        println(">>> Store remove [key=" + key + ", xid=" + (if (tx == null) null else tx.xid) + ']')

        dummyDB.remove(key)
    }

    override def loadCache(clo: IgniteBiInClosure[JavaLong, Person], args: AnyRef*) {
        val cnt = args(0).asInstanceOf[Integer]

        println(">>> Store loadCache for entry count: " + cnt)

        for (i <- 0 until cnt) {
            val p = new Person(i, "first-" + i, "last-" + 1)

            if (ignite.affinity(cacheName).isPrimaryOrBackup(ignite.cluster.localNode, p.getId)) {
                dummyDB.put(p.getId, p)

                clo.apply(p.getId, p)
            }
        }
    }

    /**
     * @return Current transaction.
     */
    @Nullable private def transaction: Transaction = {
        if (ses != null) ses.transaction else null
    }
}
