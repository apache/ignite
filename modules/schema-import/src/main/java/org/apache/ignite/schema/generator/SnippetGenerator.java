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

package org.apache.ignite.schema.generator;

import org.apache.ignite.schema.model.*;
import org.apache.ignite.schema.ui.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.schema.ui.MessageBox.Result.*;

/**
 * Cache configuration snippet generator.
 */
public class SnippetGenerator {
    /**
     * Add type fields.
     *
     * @param src Source code lines.
     * @param owner Fields owner collection.
     * @param fields Fields metadata.
     */
    private static void addFields(Collection<String> src, String owner, Collection<PojoField> fields) {
        for (PojoField field : fields) {
            String javaTypeName = field.javaTypeName();

            if (javaTypeName.startsWith("java.lang."))
                javaTypeName = javaTypeName.substring(10);

            src.add(owner + ".add(new CacheTypeFieldMetadata(\"" + field.dbName() + "\", " +
                "java.sql.Types." + field.dbTypeName() + ", \"" +
                field.javaName() + "\", " + javaTypeName + ".class));");
        }
    }

    /**
     * Generate java snippet for cache configuration with JDBC store.
     *
     * @param pojos POJO descriptors.
     * @param pkg Types package.
     * @param includeKeys {@code true} if key fields should be included into value class.
     * @param out File to output snippet.
     * @param askOverwrite Callback to ask user to confirm file overwrite.
     * @throws IOException If generation failed.
     */
    public static void generate(Collection<PojoDescriptor> pojos, String pkg, boolean includeKeys, File out,
        ConfirmCallable askOverwrite) throws IOException {
        if (out.exists()) {
            MessageBox.Result choice = askOverwrite.confirm(out.getName());

            if (CANCEL == choice)
                throw new IllegalStateException("Java configuration snippet generation was canceled!");

            if (NO == choice || NO_TO_ALL == choice)
                return;
        }

        Collection<String> src = new ArrayList<>(256);

        src.add("// Code snippet for cache configuration.");
        src.add("");
        src.add("IgniteConfiguration cfg = new IgniteConfiguration();");
        src.add("");
        src.add("CacheConfiguration ccfg = new CacheConfiguration<>();");
        src.add("");
        src.add("DataSource dataSource = null; // TODO: Create data source for your database.");
        src.add("");
        src.add("// Create store. ");
        src.add("CacheJdbcPojoStore store = new CacheJdbcPojoStore();");
        src.add("store.setDataSource(dataSource);");
        src.add("");
        src.add("// Create store factory. ");
        src.add("ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory<>(store));");
        src.add("");
        src.add("// Configure cache to use store. ");
        src.add("ccfg.setReadThrough(true);");
        src.add("ccfg.setWriteThrough(true);");
        src.add("");
        src.add("cfg.setCacheConfiguration(ccfg);");
        src.add("");
        src.add("// Configure cache types. ");
        src.add("Collection<CacheTypeMetadata> meta = new ArrayList<>();");
        src.add("");

        boolean first = true;

        for (PojoDescriptor pojo : pojos) {
            String tbl = pojo.table();

            src.add("// " + tbl + ".");
            src.add((first ? "CacheTypeMetadata " : "") +  "type = new CacheTypeMetadata();");
            src.add("type.setDatabaseSchema(\"" + pojo.schema() + "\");");
            src.add("type.setDatabaseTable(\"" + tbl + "\");");
            src.add("type.setKeyType(\"" + pkg + "." + pojo.keyClassName() + "\");");
            src.add("type.setValueType(\"" +  pkg + "." + pojo.valueClassName() + "\");");
            src.add("");

            src.add("// Key fields for " + tbl + ".");
            src.add((first ? "Collection<CacheTypeFieldMetadata> " : "") + "keys = new ArrayList<>();");
            addFields(src, "keys", pojo.keyFields());
            src.add("type.setKeyFields(keys);");
            src.add("");

            src.add("// Value fields for " + tbl + ".");
            src.add((first ? "Collection<CacheTypeFieldMetadata> " : "") + "vals = new ArrayList<>();");
            addFields(src, "vals", pojo.valueFields(includeKeys));
            src.add("type.setValueFields(vals);");
            src.add("");

            first = false;
        }

        src.add("// Start Ignite node.");
        src.add("Ignition.start(cfg);");

        // Write generated code to file.
        try (Writer writer = new BufferedWriter(new FileWriter(out))) {
            for (String line : src)
                writer.write(line + '\n');
        }
    }
}
