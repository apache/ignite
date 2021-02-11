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

package org.apache.ignite.tests;

import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.ignite.tests.utils.TestsHelper;

/**
 * Recreates all required Cassandra database objects (keyspace, table, indexes) for load tests
 */
public class LoadTestsCassandraArtifactsCreator {
    /**
     * Recreates Cassandra artifacts required for load tests
     * @param args not used
     */
    public static void main(String[] args) {
        try {
            System.out.println("[INFO] Recreating Cassandra artifacts (keyspace, table, indexes) for load tests");

            KeyValuePersistenceSettings perSettings =
                    new KeyValuePersistenceSettings(TestsHelper.getLoadTestsPersistenceSettings());

            System.out.println("[INFO] Dropping test keyspace: " + perSettings.getKeyspace());

            try {
                CassandraHelper.dropTestKeyspaces();
            } catch (Throwable e) {
                throw new RuntimeException("Failed to drop test keyspace: " + perSettings.getKeyspace(), e);
            }

            System.out.println("[INFO] Test keyspace '" + perSettings.getKeyspace() + "' was successfully dropped");

            System.out.println("[INFO] Creating test keyspace: " + perSettings.getKeyspace());

            try {
                CassandraHelper.executeWithAdminCredentials(perSettings.getKeyspaceDDLStatement());
            } catch (Throwable e) {
                throw new RuntimeException("Failed to create test keyspace: " + perSettings.getKeyspace(), e);
            }

            System.out.println("[INFO] Test keyspace '" + perSettings.getKeyspace() + "' was successfully created");

            System.out.println("[INFO] Creating test table: " + perSettings.getTable());

            try {
                CassandraHelper.executeWithAdminCredentials(perSettings.getTableDDLStatement(perSettings.getTable()));
            } catch (Throwable e) {
                throw new RuntimeException("Failed to create test table: " + perSettings.getTable(), e);
            }

            System.out.println("[INFO] Test table '" + perSettings.getTable() + "' was successfully created");

            List<String> statements = perSettings.getIndexDDLStatements(perSettings.getTable());
            if (statements == null)
                statements = new LinkedList<>();

            for (String statement : statements) {
                System.out.println("[INFO] Creating test table index:");
                System.out.println(statement);

                try {
                    CassandraHelper.executeWithAdminCredentials(statement);
                } catch (Throwable e) {
                    throw new RuntimeException("Failed to create test table index", e);
                }

                System.out.println("[INFO] Test table index was successfully created");
            }

            System.out.println("[INFO] All required Cassandra artifacts were successfully recreated");
        }
        catch (Throwable e) {
            System.out.println("[ERROR] Failed to recreate Cassandra artifacts");
            e.printStackTrace(System.out);

            if (e instanceof RuntimeException)
                throw (RuntimeException)e;
            else
                throw new RuntimeException(e);
        }
        finally {
            CassandraHelper.releaseCassandraResources();
        }
    }
}
