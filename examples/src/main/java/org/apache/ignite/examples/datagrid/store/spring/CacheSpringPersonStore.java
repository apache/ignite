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

package org.apache.ignite.examples.datagrid.store.spring;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.examples.datagrid.store.Person;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Example of {@link CacheStore} implementation that uses JDBC
 * transaction with cache transactions and maps {@link Long} to {@link Person}.
 */
public class CacheSpringPersonStore extends CacheStoreAdapter<Long, Person> {
    /** Data source. */
    public static final DataSource DATA_SRC = new DriverManagerDataSource("jdbc:h2:mem:example;DB_CLOSE_DELAY=-1");

    /** Spring JDBC template. */
    private JdbcTemplate jdbcTemplate;

    /**
     * Constructor.
     *
     * @throws IgniteException If failed.
     */
    public CacheSpringPersonStore() throws IgniteException {
        jdbcTemplate = new JdbcTemplate(DATA_SRC);

        prepareDb();
    }

    /**
     * Prepares database for example execution. This method will create a
     * table called "PERSONS" so it can be used by store implementation.
     *
     * @throws IgniteException If failed.
     */
    private void prepareDb() throws IgniteException {
        jdbcTemplate.update(
            "create table if not exists PERSONS (" +
            "id number unique, firstName varchar(255), lastName varchar(255))");
    }

    /** {@inheritDoc} */
    @Override public Person load(Long key) {
        System.out.println(">>> Store load [key=" + key + ']');

        try {
            return jdbcTemplate.queryForObject("select * from PERSONS where id = ?", new RowMapper<Person>() {
                @Override public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return new Person(rs.getLong(1), rs.getString(2), rs.getString(3));
                }
            }, key);
        }
        catch (EmptyResultDataAccessException ignored) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends Long, ? extends Person> entry) {
        Long key = entry.getKey();
        Person val = entry.getValue();

        System.out.println(">>> Store write [key=" + key + ", val=" + val + ']');

        int updated = jdbcTemplate.update("update PERSONS set firstName = ?, lastName = ? where id = ?",
            val.getFirstName(), val.getLastName(), val.getId());

        if (updated == 0) {
            jdbcTemplate.update("insert into PERSONS (id, firstName, lastName) values (?, ?, ?)",
                val.getId(), val.getFirstName(), val.getLastName());
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        System.out.println(">>> Store delete [key=" + key + ']');

        jdbcTemplate.update("delete from PERSONS where id = ?", key);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<Long, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        int entryCnt = (Integer)args[0];

        final AtomicInteger cnt = new AtomicInteger();

        jdbcTemplate.query("select * from PERSONS limit ?", new RowCallbackHandler() {
            @Override public void processRow(ResultSet rs) throws SQLException {
                Person person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3));

                clo.apply(person.getId(), person);

                cnt.incrementAndGet();
            }
        }, entryCnt);

        System.out.println(">>> Loaded " + cnt + " values into cache.");
    }
}