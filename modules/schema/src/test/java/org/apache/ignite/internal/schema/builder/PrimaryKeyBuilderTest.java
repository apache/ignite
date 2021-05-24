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

import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.builder.PrimaryIndexBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Primary key builder test.
 */
public class PrimaryKeyBuilderTest {
    /** Test primary index parameters. */
    @Test
    public void testPrimaryKey() {
        PrimaryIndexBuilder builder = SchemaBuilders.pkIndex();

        builder.addIndexColumn("A").desc().done();
        builder.addIndexColumn("B").asc().done();

        PrimaryIndex idx = builder.build();

        assertEquals(2, idx.columns().size());
        assertFalse(idx.columns().get(0).asc());
        assertTrue(idx.columns().get(1).asc());
    }
}
