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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.AbstractH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheUnionDuplicatesTest extends AbstractH2CompareQueryTest {
    /** {@inheritDoc} */
    @Override protected void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) {
        if (mode == PARTITIONED)
            cc.setIndexedTypes(Integer.class, Organization.class);
    }

    /** {@inheritDoc} */
    @Override protected void initCacheAndDbData() throws Exception {
        Integer k1 = primaryKey(ignite(0).cache(pCache.getName()));
        Integer k2 = primaryKey(ignite(1).cache(pCache.getName()));

        Organization org1 = new Organization(k1, "org", "org1");
        Organization org2 = new Organization(k2, "org", "org2");

        pCache.put(k1, org1);
        pCache.put(k2, org2);

        insertInDb(org1);
        insertInDb(org2);
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {
        // No-op.
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnionDuplicateFilter() throws Exception {
        compareQueryRes0(pCache, "select name from \"part\".Organization " +
            "union " +
            "select name2 from \"part\".Organization");
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = super.initializeH2Schema();

        st.execute("create table \"part\".ORGANIZATION" +
            "  (_key int not null," +
            "  _val other not null," +
            "  id int unique," +
            "  name varchar(255)," +
            "  name2 varchar(255))");

        return st;
    }

    /**
     * Insert {@link BaseH2CompareQueryTest.Organization} at h2 database.
     *
     * @param org Organization.
     * @throws SQLException If exception.
     */
    private void insertInDb(Organization org) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement(
            "insert into \"part\".ORGANIZATION (_key, _val, id, name, name2) values(?, ?, ?, ?, ?)")) {
            st.setObject(1, org.id);
            st.setObject(2, org);
            st.setObject(3, org.id);
            st.setObject(4, org.name);
            st.setObject(5, org.name2);

            st.executeUpdate();
        }
    }

    /**
     * Organization class. Stored at partitioned cache.
     */
    private static class Organization implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        @QuerySqlField(index = true)
        private String name2;

        /**
         * Create Organization.
         *
         * @param id Organization ID.
         * @param name Organization name.
         * @param name2 Name2.
         */
        Organization(int id, String name, String name2) {
            this.id = id;
            this.name = name;
            this.name2 = name2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof Organization && id == ((Organization)o).id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Organization [id=" + id +
                ", name=" + name +
                ", name2=" + name2 + ']';
        }
    }
}
