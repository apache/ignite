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

package org.apache.ignite.cache.store.cassandra.utils;

import java.io.File;
import java.util.List;

import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;

/**
 * Generates Cassandra DDL statements from persistence descriptor xml file.
 */
public class DDLGenerator {
    /**
     * DDLGenerator entry point.
     *
     * @param args Arguments for DDLGenerator.
     */
    public static void main(String[] args) {
        if (args == null || args.length == 0)
            return;

        boolean success = true;

        for (String arg : args) {
            File file = new File(arg);
            if (!file.isFile()) {
                success = false;
                System.out.println("-------------------------------------------------------------");
                System.out.println("Incorrect file specified: " + arg);
                System.out.println("-------------------------------------------------------------");
                continue;
            }

            try {
                KeyValuePersistenceSettings settings = new KeyValuePersistenceSettings(file);
                String table = settings.getTable() != null ? settings.getTable() : "my_table";

                System.out.println("-------------------------------------------------------------");
                System.out.println("DDL for keyspace/table from file: " + arg);
                System.out.println("-------------------------------------------------------------");
                System.out.println();
                System.out.println(settings.getKeyspaceDDLStatement());
                System.out.println();
                System.out.println(settings.getTableDDLStatement(table));
                System.out.println();

                List<String> statements = settings.getIndexDDLStatements(table);
                if (statements != null && !statements.isEmpty()) {
                    for (String st : statements) {
                        System.out.println(st);
                        System.out.println();
                    }
                }
            }
            catch (Throwable e) {
                success = false;
                System.out.println("-------------------------------------------------------------");
                System.out.println("Invalid file specified: " + arg);
                System.out.println("-------------------------------------------------------------");
                e.printStackTrace();
            }
        }

        if (!success)
            throw new RuntimeException("Failed to process some of the specified files");
    }
}
