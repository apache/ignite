package org.apache.ignite.tests;

import junit.framework.TestCase;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.cassandra.persistence.KeyPersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;

public class CassandraConfigTest extends TestCase {
    /**
     * Check if same DDL generated for similar keys and same KeyPersistenceConfiguration.
     *
     * @throws Exception
     */
    public void testDDLGeneration() throws Exception {
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
