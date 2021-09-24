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

import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.index.SortOrder;
import org.apache.ignite.schema.definition.index.SortedIndexDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for sorted index builder.
 */
public class SortedIndexDefinitionBuilderTest {
    /**
     * Build sorted index and check it's parameters.
     */
    @Test
    public void testBuild() {
        SortedIndexDefinitionBuilder builder = SchemaBuilders.sortedIndex("SIDX");

        builder.addIndexColumn("A").asc().done();
        builder.addIndexColumn("B").desc().done();

        SortedIndexDefinition idx = builder.build();

        assertFalse(idx.unique());
        assertEquals(2, idx.indexedColumns().size());
        assertEquals(SortOrder.ASC, idx.columns().get(0).sortOrder());
        assertEquals(SortOrder.DESC, idx.columns().get(1).sortOrder());
    }
}
