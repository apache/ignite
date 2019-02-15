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

package org.apache.ignite.tests;

import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.cassandra.persistence.KeyPersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Simple test for DDL generator.
 */
public class CassandraConfigTest {
    /**
     * Check if same DDL generated for similar keys and same KeyPersistenceConfiguration.
     */
    @Test
    public void testDDLGeneration() {
        KeyPersistenceSettings keyPersistenceSettingsA = getKeyPersistenceSettings(KeyA.class);
        KeyPersistenceSettings keyPersistenceSettingsB = getKeyPersistenceSettings(KeyB.class);

        assertEquals(keyPersistenceSettingsB.getPrimaryKeyDDL(),
            keyPersistenceSettingsA.getPrimaryKeyDDL());

        assertEquals(keyPersistenceSettingsB.getClusteringDDL(),
            keyPersistenceSettingsA.getClusteringDDL());
    }

    /**
     * @return KeyPersistenceSetting
     */
    private KeyPersistenceSettings getKeyPersistenceSettings(Class keyClass) {
        String cfg = "<persistence keyspace=\"public\">" +
            " <keyPersistence class=\"" + keyClass.getName() + "\"  strategy=\"POJO\"> \n" +
            "        <partitionKey>\n" +
            "            <field name=\"name\" column=\"name\"/>\n" +
            "            <field name=\"contextId\" column=\"context_id\"/>\n" +
            "            <field name=\"creationDate\" column=\"creation_date\"/>\n" +
            "        </partitionKey>\n" +
            "        <clusterKey>\n" +
            "            <field name=\"timestamp\" column=\"timestamp\"/>\n" +
            "        </clusterKey>\n" +
            "    </keyPersistence>" +
            " <valuePersistence class=\"java.lang.Object\"  strategy=\"BLOB\">" +
            " </valuePersistence>" +
            "</persistence>";

        return new KeyValuePersistenceSettings(cfg).getKeyPersistenceSettings();
    }

    /**
     *
     */
    public static class BaseKey {
        /** */
        @QuerySqlField
        // Looks like next annotation is ignored when generating DDL,
        // but Ignite supports this annotation in parent classes.
//        @AffinityKeyMapped
        private Integer contextId;

        /** */
        public Integer getContextId() {
            return contextId;
        }

        /** */
        public void setContextId(Integer contextId) {
            this.contextId = contextId;
        }
    }

    /**
     *
     */
    public static class KeyA extends BaseKey {
        /** */
        @QuerySqlField(index = true)
        private String timestamp;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        @QuerySqlField
        private String creationDate;

        /**
         * Constructor.
         */
        public KeyA() {
        }
    }

    /**
     *
     */
    public static class KeyB {

        /** */
        @QuerySqlField(index = true)
        private String timestamp;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        @QuerySqlField
        private String creationDate;

        /** */
        @QuerySqlField
//        @AffinityKeyMapped
        private Integer contextId;

        /**
         * Constructor.
         */
        public KeyB() {
        }
    }
}
