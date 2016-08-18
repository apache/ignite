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

package org.apache.ignite.internal.processors.query.h2.ext.func;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.AbstractH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base set of queries to compare query results from h2 database instance and mixed ignite caches (replicated and
 * partitioned) which have the same data models and data content.
 */
public class H2ExtFunctionsQueryTest extends AbstractH2CompareQueryTest {

    /** Person count. */
    private static final int PERSON_CNT = 5;

    /** {@inheritDoc} */
    @Override protected void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) {
        cc.setSqlFunctionClasses(Functions.class);

        if (mode == CacheMode.PARTITIONED)
            cc.setIndexedTypes(
                Integer.class, Person.class
            );
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void initCacheAndDbData() throws SQLException {
        Collection<Person> persons = new ArrayList<>();

        for (int id = 0; id < PERSON_CNT; id++) {

            Person person = new Person(id, "name" + id, "lastName" + id);

            // Add a Person without lastName.
            if (id == 0)
                person.lastName = null;

            persons.add(person);

            pCache.put(person.id, person);

            insertInDb(person);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {

    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testSqlLenFunctionInSqlFieldsQuery() throws Exception {
        final SqlFieldsQuery sql = new SqlFieldsQuery("SELECT firstName, lastName, LEN(concat(firstName, ' ', lastName)) from Person;");

        List<List<?>> rows = pCache.query(sql).getAll();

        assertEquals(PERSON_CNT, rows.size());

        for (List row : rows) {
            assertEquals(3, row.size());

            String firstName = row.get(0) == null ? "" : (String)row.get(0);
            String lastName = row.get(1) == null ? "" : (String)row.get(1);
            Long size = (Long)row.get(2);

            assertNotNull(size);

            String fullName = firstName + " " + lastName;

            assertEquals(Integer.valueOf(fullName.length()).longValue(), size.longValue());
        }
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = super.initializeH2Schema();

        st.execute("create table \"part\".PERSON" +
            "  (_key int not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  firstName varchar(255), " +
            "  lastName varchar(255) )");

        conn.commit();

        return st;
    }

    /**
     * Insert {@link Person} at h2 database.
     *
     * @param p Person.
     * @throws SQLException If exception.
     */
    private void insertInDb(Person p) throws SQLException {
        try (PreparedStatement st = conn.prepareStatement("insert into \"part\".PERSON " +
            "(_key, _val, id, firstName, lastName) values(?, ?, ?, ?, ?)")) {
            st.setObject(1, p.id);
            st.setObject(2, p);
            st.setObject(3, p.id);
            st.setObject(4, p.firstName);
            st.setObject(5, p.lastName);

            st.executeUpdate();
        }
    }

    /**
     * Person class. Stored at partitioned cache.
     */
    private static class Person implements Serializable {
        /** Person ID (indexed). */
        @QuerySqlField(index = true)
        private int id;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /**
         * Constructs person record.
         *
         * @param firstName First name.
         * @param lastName Last name.
         */
        Person(int id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Person && id == ((Person)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Person [firstName=" + firstName +
                ", lastName=" + lastName +
                ", id=" + id + ']';
        }
    }
}