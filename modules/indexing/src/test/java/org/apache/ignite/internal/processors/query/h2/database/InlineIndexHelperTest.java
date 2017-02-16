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

/**
 * Simple tests for {@link InlineIndexHelper}.
 */
public class InlineIndexHelperTest extends TestCase {

    /** Test utf-8 string cutting. */
    public void testConvert() {
        // 8 bytes total: 1b, 1b, 3b, 3b.

        byte[] bytes = InlineIndexHelper.toBytes("00\u20ac\u20ac", 7);
        assertEquals(5, bytes.length);

        String s = new String(bytes);
        assertEquals(3, s.length());
    }

    /** Limit is too small to cut */
    public void testShort() {
        // 6 bytes total: 3b, 3b.

        byte[] bytes = InlineIndexHelper.toBytes("\u20ac\u20ac", 2);
        assertNull(bytes);
    }

}