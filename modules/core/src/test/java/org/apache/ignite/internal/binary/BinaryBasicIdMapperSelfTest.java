/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.internal.binary.test.GridBinaryTestClass1;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class BinaryBasicIdMapperSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLowerCase() throws Exception {
        BinaryBasicIdMapper mapper = new BinaryBasicIdMapper(true);

        assertEquals(GridBinaryTestClass1.class.getName().toLowerCase().hashCode(),
            mapper.typeId(GridBinaryTestClass1.class.getName()));
        assertEquals((GridBinaryTestClass1.class.getName() + "$InnerClass").toLowerCase().hashCode(),
            mapper.typeId(GridBinaryTestClass1.class.getName() + "$InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultCase() throws Exception {
        BinaryBasicIdMapper mapper = new BinaryBasicIdMapper(false);

        assertEquals(GridBinaryTestClass1.class.getName().hashCode(),
            mapper.typeId(GridBinaryTestClass1.class.getName()));
        assertEquals((GridBinaryTestClass1.class.getName() + "$InnerClass").hashCode(),
            mapper.typeId(GridBinaryTestClass1.class.getName() + "$InnerClass"));
    }
}
