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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.engine.SysProperties;
import org.junit.Test;
import org.junit.Before;

/**
 * Test to check the values of SysProperties variables are correct
 */
public class IgniteH2PropertiesTest extends GridCommonAbstractTest {
    /** Instance name. */
    private static final String INSTANCE_NAME = "test_node";

    /** */
    @Before
    public void setUp() throws Exception {
        try {
            startGrid(INSTANCE_NAME, "examples/config/example-ignite.xml");
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteH2Properties() {
        assertFalse(SysProperties.OBJECT_CACHE);
        assertFalse(SysProperties.serializeJavaObject);

        assertTrue(SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE == 0);
        assertTrue("false".equals(System.getProperty("h2.optimizeTwoEquals")));
        assertTrue("false".equals(System.getProperty("h2.dropRestrict")));
    }
}
