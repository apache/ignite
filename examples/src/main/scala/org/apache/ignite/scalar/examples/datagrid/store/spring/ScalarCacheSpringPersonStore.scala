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

package org.apache.ignite.scalar.examples.datagrid.store.spring

import java.lang.{Long => JavaLong}
import java.sql.{ResultSet, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import javax.cache.Cache
import javax.cache.integration.CacheLoaderException
import javax.sql.DataSource

import org.apache.ignite.cache.store.CacheStoreAdapter
import org.apache.ignite.lang.IgniteBiInClosure
import org.apache.ignite.scalar.examples.datagrid.store.spring.ScalarCacheSpringPersonStore._
import org.apache.ignite.scalar.examples.model.Person
import org.h2.jdbcx.JdbcConnectionPool
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.{JdbcTemplate, RowCallbackHandler, RowMapper}

/**
 * Dummy cache store implementation.
 */
class ScalarCacheSpringPersonStore extends CacheStoreAdapter[JavaLong, Person] {
    /** Spring JDBC template. */
    private var jdbcTemplate = new JdbcTemplate(DATA_SRC)

    override def load(key: JavaLong): Person = {
        System.out.println(">>> Store load [key=" + key + ']')

        try {
            jdbcTemplate.queryForObject("select * from PERSON where id = ?", new RowMapper[Person]() {
                override def mapRow(rs: ResultSet, rowNum: Int): Person = {
                    new Person(rs.getLong(1), rs.getString(2), rs.getString(3))
                }
            }, key)
        }
        catch {
            case ignored: EmptyResultDataAccessException => null
        }
    }

    override def write(entry: Cache.Entry[_ <: JavaLong, _ <: Person]) {
        val key = entry.getKey
        val value = entry.getValue

        System.out.println(">>> Store write [key=" + key + ", val=" + value + ']')

        val updated = jdbcTemplate.update("update PERSON set first_name = ?, last_name = ? where id = ?", value.getFirstName, value.getLastName, new JavaLong(value.getId))

        if (updated == 0)
            jdbcTemplate.update("insert into PERSON (id, first_name, last_name) values (?, ?, ?)", new JavaLong(value.getId), value.getFirstName, value.getLastName)
    }

    override def delete(key: AnyRef) {
        System.out.println(">>> Store delete [key=" + key + ']')

        jdbcTemplate.update("delete from PERSON where id = ?", key)
    }

    override def loadCache(clo: IgniteBiInClosure[JavaLong, Person], args: AnyRef*) {
        if (args == null || args.isEmpty || args(0) == null) throw new CacheLoaderException("Expected entry count parameter is not provided.")

        val entryCnt = args(0).asInstanceOf[Integer]

        val cnt = new AtomicInteger

        jdbcTemplate.query("select * from PERSON limit ?", new RowCallbackHandler() {
            @throws(classOf[SQLException])
            override def processRow(rs: ResultSet) {
                val person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3))

                clo.apply(person.getId, person)

                cnt.incrementAndGet
            }
        }, entryCnt)

        System.out.println(">>> Loaded " + cnt + " values into cache.")
    }
}

object ScalarCacheSpringPersonStore {
    /** Data source. */
    val DATA_SRC: DataSource = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", "")
}
