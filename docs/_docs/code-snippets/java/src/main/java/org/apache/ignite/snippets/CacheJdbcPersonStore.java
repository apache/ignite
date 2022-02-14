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
package org.apache.ignite.snippets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;

//tag::class[]
public class CacheJdbcPersonStore extends CacheStoreAdapter<Long, Person> {
    // This method is called whenever the "get(...)" methods are called on IgniteCache.
    @Override
    public Person load(Long key) {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from PERSON where id=?")) {
                st.setLong(1, key);

                ResultSet rs = st.executeQuery();

                return rs.next() ? new Person(rs.getInt(1), rs.getString(2)) : null;
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load: " + key, e);
        }
    }

    @Override
    public void write(Entry<? extends Long, ? extends Person> entry) throws CacheWriterException {
        try (Connection conn = connection()) {
            // Syntax of MERGE statement is database specific and should be adopted for your database.
            // If your database does not support MERGE statement then use sequentially
            // update, insert statements.
            try (PreparedStatement st = conn.prepareStatement("merge into PERSON (id, name) key (id) VALUES (?, ?)")) {
                Person val = entry.getValue();

                st.setLong(1, entry.getKey());
                st.setString(2, val.getName());

                st.executeUpdate();
            }
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to write entry (" + entry + ")", e);
        }
    }

    // This method is called whenever the "remove(...)" method are called on IgniteCache.
    @Override
    public void delete(Object key) {
        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("delete from PERSON where id=?")) {
                st.setLong(1, (Long) key);

                st.executeUpdate();
            }
        } catch (SQLException e) {
            throw new CacheWriterException("Failed to delete: " + key, e);
        }
    }

    // This method is called whenever the "loadCache()" and "localLoadCache()"
    // methods are called on IgniteCache. It is used for bulk-loading the cache.
    // If you don't need to bulk-load the cache, skip this method.
    @Override
    public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer) args[0];

        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from PERSON")) {
                try (ResultSet rs = st.executeQuery()) {
                    int cnt = 0;

                    while (cnt < entryCnt && rs.next()) {
                        Person person = new Person(rs.getInt(1), rs.getString(2));
                        clo.apply(person.getId(), person);
                        cnt++;
                    }
                }
            }
        } catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }

    // Open JDBC connection.
    private Connection connection() throws SQLException {
        // Open connection to your RDBMS systems (Oracle, MySQL, Postgres, DB2, Microsoft SQL, etc.)
        Connection conn = DriverManager.getConnection("jdbc:mysql://[host]:[port]/[database]", "YOUR_USER_NAME", "YOUR_PASSWORD");

        conn.setAutoCommit(true);

        return conn;
    }
}

//end::class[]
