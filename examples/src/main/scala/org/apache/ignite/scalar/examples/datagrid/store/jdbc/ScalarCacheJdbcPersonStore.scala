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

package org.apache.ignite.scalar.examples.datagrid.store.jdbc

import org.apache.ignite.IgniteException
import org.apache.ignite.cache.store.{CacheStore, CacheStoreAdapter, CacheStoreSession}
import org.apache.ignite.lang.IgniteBiInClosure
import org.apache.ignite.resources.CacheStoreSessionResource
import org.apache.ignite.scalar.examples.datagrid.store._

import org.jetbrains.annotations.Nullable

import javax.cache.Cache
import javax.cache.integration.{CacheLoaderException, CacheWriterException}
import java.lang.{Long => JavaLong}
import java.sql._

/**
 * Example of [[CacheStore]] implementation that uses JDBC
 * transaction with cache transactions and maps [[java.lang.Long]] to [[Person]].
 *
 */
class ScalarCacheJdbcPersonStore extends CacheStoreAdapter[JavaLong, Person] {
    /** Transaction metadata attribute name. */
    private val ATTR_NAME = "SIMPLE_STORE_CONNECTION"
    
    /** Auto-injected store session. */
    @CacheStoreSessionResource private var ses: CacheStoreSession = null

    prepareDb()

    /**
     * Prepares database for example execution. This method will create a
     * table called "PERSONS" so it can be used by store implementation.
     */
    private def prepareDb() {
        val conn = openConnection(false)

        val st = conn.createStatement

        try {
            st.execute("create table if not exists PERSONS (id number unique, firstName varchar(255), " + "lastName varchar(255))")
            
            conn.commit()
        }
        catch {
            case e: SQLException =>
                throw new IgniteException("Failed to create database table.", e)
        }
        finally {
            if (conn != null) 
                conn.close()
            if (st != null) 
                st.close()
        }
    }

    override def sessionEnd(commit: Boolean) {
        val props = ses.properties[String, Connection]()
        
        val conn: Connection = props.remove(ATTR_NAME)

        try {
            if (conn != null) {
                if (commit) 
                    conn.commit()
                else 
                    conn.rollback()
            }

            println(">>> Transaction ended [commit=" + commit + ']')
        }
        catch {
            case e: SQLException =>
                throw new CacheWriterException("Failed to end transaction: " + ses.transaction, e)
        }
        finally {
            if (conn != null) conn.close()
        }
    }

    override def load(key: JavaLong): Person = {
        println(">>> Loading key: " + key)

        var conn: Connection = null

        try {
            conn = connection()

            val st = conn.prepareStatement("select * from PERSONS where id=?")

            try {
                st.setString(1, key.toString)

                val rs = st.executeQuery

                if (rs.next)
                    return new Person(rs.getLong(1), rs.getString(2), rs.getString(3))
            }
            finally {
                if (st != null)
                    st.close()
            }
        }
        catch {
            case e: SQLException =>
                throw new CacheLoaderException("Failed to load object: " + key, e)
        }
        finally {
            end(conn)
        }

        null
    }

    def write(entry: Cache.Entry[_ <: JavaLong, _ <: Person]) {
        val key = entry.getKey

        val value = entry.getValue

        println(">>> Putting [key=" + key + ", val=" + value + ']')

        var conn: Connection = null

        try {
            conn = connection()

            var updated = 0

            val st = conn.prepareStatement("update PERSONS set firstName=?, lastName=? where id=?")

            try {
                st.setString(1, value.getFirstName)
                st.setString(2, value.getLastName)
                st.setLong(3, value.getId)

                updated = st.executeUpdate
            }
            finally {
                if (st != null)
                    st.close()
            }

            if (updated == 0) {
                val st = conn.prepareStatement("insert into PERSONS (id, firstName, lastName) values(?, ?, ?)")

                try {
                    st.setLong(1, value.getId)
                    st.setString(2, value.getFirstName)
                    st.setString(3, value.getLastName)

                    st.executeUpdate
                }
                finally {
                    if (st != null)
                        st.close()
                }
            }
        }
        catch {
            case e: SQLException =>
                throw new CacheLoaderException("Failed to put object [key=" + key + ", val=" + value + ']', e)
        }
        finally {
            end(conn)
        }
    }

    def delete(key: AnyRef) {
        println(">>> Removing key: " + key)

        var conn: Connection = null

        try {
            conn = connection()

            val st = conn.prepareStatement("delete from PERSONS where id=?")

            try {
                st.setLong(1, key.asInstanceOf[JavaLong])

                st.executeUpdate
            }
            finally {
                if (st != null)
                    st.close()
            }
        }
        catch {
            case e: SQLException =>
                throw new CacheWriterException("Failed to remove object: " + key, e)
        }
        finally {
            end(conn)
        }
    }

    override def loadCache(clo: IgniteBiInClosure[JavaLong, Person], args: AnyRef*) {
        if (args == null || args.length == 0 || args(0) == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.")

        val entryCnt = args(0).asInstanceOf[Integer]

        val conn = connection()

        try {
            val st = conn.prepareStatement("select * from PERSONS")

            try {
                val rs = st.executeQuery

                try {
                    var cnt = 0

                    while (cnt < entryCnt && rs.next) {
                        val person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3))

                        clo.apply(person.getId, person)

                        cnt += 1
                    }

                    println(">>> Loaded " + cnt + " values into cache.")
                }
                finally {
                    if (rs != null)
                        rs.close()
                }
            }
            finally {
                if (st != null)
                    st.close()
            }
        }
        catch {
            case e: SQLException =>
                throw new CacheLoaderException("Failed to load values from cache store.", e)
        }
        finally {
            if (conn != null) conn.close()
        }
    }

    /**
     * @return Connection.
     */
    private def connection(): Connection = {
        if (ses.isWithinTransaction) {
            val props = ses.properties[AnyRef, AnyRef]()

            var conn = props.get(ATTR_NAME).asInstanceOf[Connection]

            if (conn == null) {
                conn = openConnection(false)

                props.put(ATTR_NAME, conn)
            }

            conn
        }
        else
            openConnection(true)
    }

    /**
     * Closes allocated resources depending on transaction status.
     *
     * @param conn Allocated connection.
     */
    private def end(@Nullable conn: Connection) {
        if (!ses.isWithinTransaction && conn != null) {
            try {
                conn.close()
            }
            catch {
                case ignored: SQLException =>
            }
        }
    }

    /**
     * Gets connection from a pool.
     *
     * @param autocommit { @code true} If connection should use autocommit mode.
     * @return Pooled connection.
     */
    private def openConnection(autocommit: Boolean): Connection = {
        val conn = DriverManager.getConnection("jdbc:h2:mem:example;DB_CLOSE_DELAY=-1")

        conn.setAutoCommit(autocommit)

        conn
    }
}
