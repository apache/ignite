/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.h2.jdbcx.JdbcConnectionPool;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

/**
 * Example of {@link CacheStore} implementation that uses JDBC
 * transaction with cache transactions and maps {@link Long} to {@link Person}.
 */
public class CacheSpringPersonStore extends CacheStoreAdapter<Long, Person> {
    /** Data source. */
    public static final DataSource DATA_SRC =
        JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", "");

    /** Spring JDBC template. */
    private JdbcTemplate jdbcTemplate;

    /**
     * Constructor.
     *
     * @throws IgniteException If failed.
     */
    public CacheSpringPersonStore() throws IgniteException {
        jdbcTemplate = new JdbcTemplate(DATA_SRC);
    }

    /** {@inheritDoc} */
    @Override public Person load(Long key) {
        System.out.println(">>> Store load [key=" + key + ']');

        try {
            return jdbcTemplate.queryForObject("select * from PERSON where id = ?", new RowMapper<Person>() {
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

        int updated = jdbcTemplate.update("update PERSON set first_name = ?, last_name = ? where id = ?",
            val.firstName, val.lastName, val.id);

        if (updated == 0) {
            jdbcTemplate.update("insert into PERSON (id, first_name, last_name) values (?, ?, ?)",
                val.id, val.firstName, val.lastName);
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        System.out.println(">>> Store delete [key=" + key + ']');

        jdbcTemplate.update("delete from PERSON where id = ?", key);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final IgniteBiInClosure<Long, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        int entryCnt = (Integer)args[0];

        final AtomicInteger cnt = new AtomicInteger();

        jdbcTemplate.query("select * from PERSON limit ?", new RowCallbackHandler() {
            @Override public void processRow(ResultSet rs) throws SQLException {
                Person person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3));

                clo.apply(person.id, person);

                cnt.incrementAndGet();
            }
        }, entryCnt);

        System.out.println(">>> Loaded " + cnt + " values into cache.");
    }
}
