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

package org.apache.ignite.internal.schema.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.builder.ColumnDefinitionBuilder;
import org.junit.jupiter.api.Test;

/**
 * Tests for table column builder.
 */
public class ColumnDefinitionBuilderTest {
    /**
     * Check column parameters.
     */
    @Test
    public void testCreateColumn() {
        ColumnDefinitionBuilder builder = SchemaBuilders.column("TEST", ColumnType.DOUBLE);

        ColumnDefinition col = builder.asNonNull().withDefaultValueExpression("NOW()").build();

        assertEquals("TEST", col.name());
        assertEquals(ColumnType.DOUBLE, col.type());
        assertEquals("NOW()", col.defaultValue());
        assertFalse(col.nullable());
    }
}
