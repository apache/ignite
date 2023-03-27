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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.internal.binary.test.GridBinaryTestClass1;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class BinaryBasicNameMapperSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleName() throws Exception {
        BinaryBasicNameMapper mapper = new BinaryBasicNameMapper(true);

        assertEquals("GridBinaryTestClass1", mapper.typeName(GridBinaryTestClass1.class.getName()));

        assertEquals("InnerClass", mapper.typeName(GridBinaryTestClass1.class.getName() + "$InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleNameDotNet() throws Exception {
        BinaryBasicNameMapper mapper = new BinaryBasicNameMapper(true);

        assertEquals("Baz", mapper.typeName("Foo.Bar.Baz"));

        assertEquals("Bar`1[[Qux]]", mapper.typeName("Foo.Bar`1[[Baz.Qux]]"));

        assertEquals("List`1[[Int32[]]]",
                mapper.typeName("System.Collections.Generic.List`1[[System.Int32[]]]"));

        assertEquals("Bar`1[[Qux`2[[String],[Int32]]]]",
                mapper.typeName("Foo.Bar`1[[Baz.Qux`2[[System.String],[System.Int32]]]]"));

        assertEquals("Bar`1[[Qux`2[[C],[Int32]]]]",
                mapper.typeName("Foo.Outer+Bar`1[[Baz.Outer2+Qux`2[[A.B+C],[System.Int32]]]]"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFullName() throws Exception {
        BinaryBasicNameMapper mapper = new BinaryBasicNameMapper(false);

        assertEquals(GridBinaryTestClass1.class.getName(), mapper.typeName(GridBinaryTestClass1.class.getName()));

        assertEquals(GridBinaryTestClass1.class.getName() + "$InnerClass",
            mapper.typeName(GridBinaryTestClass1.class.getName() + "$InnerClass"));
    }
}
