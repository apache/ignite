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

package org.apache.ignite.internal.processors.query.h2.database;

import junit.framework.TestCase;
import org.h2.result.SortOrder;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Simple tests for {@link H2Tree}.
 */
public class H2TreeSelfTest extends TestCase {

    /** Test on String values compare */
    public void testRelyOnCompare() {

        InlineIndexHelper ha = new InlineIndexHelper(Value.STRING, 0, SortOrder.ASCENDING);
        InlineIndexHelper hd = new InlineIndexHelper(Value.STRING, 0, SortOrder.DESCENDING);

        // same size
        assertFalse(H2Tree.canRelyOnCompare(0, ValueString.get("aabb"), ValueString.get("aabb"), ha));
        assertFalse(H2Tree.canRelyOnCompare(0, ValueString.get("aabb"), ValueString.get("aabb"), hd));

        // second string is shorter
        assertTrue(H2Tree.canRelyOnCompare(1, ValueString.get("aabb"), ValueString.get("aab"), ha));
        assertTrue(H2Tree.canRelyOnCompare(-1, ValueString.get("aabb"), ValueString.get("aab"), hd));

        // second string is longer
        assertTrue(H2Tree.canRelyOnCompare(1, ValueString.get("aabb"), ValueString.get("aaaaaa"), ha));
        assertTrue(H2Tree.canRelyOnCompare(-1, ValueString.get("aabb"), ValueString.get("aaaaaa"), hd));

        assertFalse(H2Tree.canRelyOnCompare(-1, ValueString.get("aab"), ValueString.get("aabbbbb"), ha));
        assertFalse(H2Tree.canRelyOnCompare(1, ValueString.get("aab"), ValueString.get("aabbbbb"), hd));

        // one is null
        assertTrue(H2Tree.canRelyOnCompare(1, ValueString.get("aabb"), ValueNull.INSTANCE, ha));
        assertTrue(H2Tree.canRelyOnCompare(-1, ValueNull.INSTANCE, ValueString.get("aab"), ha));
        assertTrue(H2Tree.canRelyOnCompare(0, ValueNull.INSTANCE, ValueNull.INSTANCE, ha));

    }

}