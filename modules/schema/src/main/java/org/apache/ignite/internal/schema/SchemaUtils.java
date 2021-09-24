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

package org.apache.ignite.internal.schema;

import java.util.Optional;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.configuration.SchemaDescriptorConverter;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.mapping.ColumnMapping;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.schema.definition.TableDefinition;

/**
 * Stateless schema utils that produces helper methods for schema preparation.
 */
public class SchemaUtils {
    /**
     * Creates schema descriptor for the table with specified configuration.
     *
     * @param schemaVer Schema version.
     * @param tblCfg Table configuration.
     * @return Schema descriptor.
     */
    public static SchemaDescriptor prepareSchemaDescriptor(int schemaVer, TableView tblCfg) {
        TableDefinition tableDef = SchemaConfigurationConverter.convert(tblCfg);

        return SchemaDescriptorConverter.convert(schemaVer, tableDef);
    }

    /**
     * Prepares column mapper.
     *
     * @param oldDesc Old schema descriptor.
     * @param oldTbl Old table configuration.
     * @param newDesc New schema descriptor.
     * @param newTbl New table configuration.
     * @return Column mapper.
     */
    public static ColumnMapper columnMapper(
        SchemaDescriptor oldDesc,
        TableView oldTbl,
        SchemaDescriptor newDesc,
        TableView newTbl
    ) {
        ColumnMapper mapper = null;

        for (String s : newTbl.columns().namedListKeys()) {
            final ColumnView newColView = newTbl.columns().get(s);
            final ColumnView oldColView = oldTbl.columns().get(s);

            if (oldColView == null && newColView != null) {
                final Column newCol = newDesc.column(newColView.name());

                assert !newDesc.isKeyColumn(newCol.schemaIndex());

                if (mapper == null)
                    mapper = ColumnMapping.createMapper(newDesc);

                mapper.add(newCol); // New column added.
            }
            else if (newColView != null) {
                final Column newCol = newDesc.column(newColView.name());
                final Column oldCol = oldDesc.column(oldColView.name());

                // TODO: IGNITE-15414 Assertion just in case, proper validation should be implemented with the help of
                // TODO: configuration validators.
                assert newCol.type().equals(oldCol.type()) :
                    LoggerMessageHelper.format(
                        "Column types doesn't match [column={}, oldType={}, newType={}",
                        oldCol.name(),
                        oldCol.type(),
                        newCol.type()
                    );

                assert newCol.nullable() == oldCol.nullable() :
                    LoggerMessageHelper.format(
                        "Column nullable properties doesn't match [column={}, oldNullable={}, newNullable={}",
                        oldCol.name(),
                        oldCol.nullable(),
                        newCol.nullable()
                    );

                if (newCol.schemaIndex() == oldCol.schemaIndex())
                    continue;

                if (mapper == null)
                    mapper = ColumnMapping.createMapper(newDesc);

                mapper.add(newCol.schemaIndex(), oldCol.schemaIndex());
            }
        }

        final Optional<Column> droppedKeyCol = oldTbl.columns().namedListKeys().stream()
            .filter(k -> newTbl.columns().get(k) == null)
            .map(k -> oldDesc.column(oldTbl.columns().get(k).name()))
            .filter(c -> oldDesc.isKeyColumn(c.schemaIndex()))
            .findAny();

        // TODO: IGNITE-15414 Assertion just in case, proper validation should be implemented with the help of
        // TODO: configuration validators.
        assert !droppedKeyCol.isPresent() :
            LoggerMessageHelper.format(
                "Dropping of key column is forbidden: [schemaVer={}, col={}]",
                newDesc.version(),
                droppedKeyCol.get()
            );

        return mapper == null ? ColumnMapping.identityMapping() : mapper;
    }

    /**
     * Compares schemas.
     *
     * @param exp Expected schema.
     * @param actual Actual schema.
     * @return {@code True} if schemas are equal, {@code false} otherwise.
     */
    public static boolean equalSchemas(SchemaDescriptor exp, SchemaDescriptor actual) {
        if (exp.keyColumns().length() != actual.keyColumns().length() ||
            exp.valueColumns().length() != actual.valueColumns().length())
            return false;

        for (int i = 0; i < exp.length(); i++) {
            if (!exp.column(i).equals(actual.column(i)))
                return false;
        }

        return true;
    }
}
