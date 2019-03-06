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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.AbstractH2CompareQueryTest;
import org.apache.ignite.internal.processors.query.h2.sql.BaseH2CompareQueryTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheUnionDuplicatesTest extends AbstractH2CompareQueryTest {
    /** */
    private static IgniteCache<Integer, Organization> pCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration("part", PARTITIONED, Integer.class, Organization.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void createCaches() {
        pCache = ignite.cache("part");
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

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        pCache = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnionDuplicateFilter() throws Exception {
        compareQueryRes0(pCache, "select name from \"part\".Organization " +
            "union " +
            "select name2 from \"part\".Organization");
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = super.initializeH2Schema();

        st.executeUpdate("CREATE SCHEMA \"part\";");

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
