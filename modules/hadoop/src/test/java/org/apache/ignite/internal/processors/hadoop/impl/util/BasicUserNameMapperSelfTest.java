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

package org.apache.ignite.internal.processors.hadoop.impl.util;

import org.apache.ignite.hadoop.util.BasicUserNameMapper;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Test for basic user name mapper.
 */
public class BasicUserNameMapperSelfTest extends GridCommonAbstractTest {
    /**
     * Test null mappings.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNullMappings() throws Exception {
        checkNullOrEmptyMappings(null);
    }

    /**
     * Test empty mappings.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyMappings() throws Exception {
        checkNullOrEmptyMappings(new HashMap<String, String>());
    }

    /**
     * Check null or empty mappings.
     *
     * @param map Mappings.
     * @throws Exception If failed.
     */
    private void checkNullOrEmptyMappings(@Nullable Map<String, String> map) throws Exception {
        BasicUserNameMapper mapper = create(map, false, null);

        assertNull(mapper.map(null));
        assertEquals("1", mapper.map("1"));
        assertEquals("2", mapper.map("2"));

        mapper = create(map, true, null);

        assertNull(mapper.map(null));
        assertNull(mapper.map("1"));
        assertNull(mapper.map("2"));

        mapper = create(map, false, "A");

        assertNull(mapper.map(null));
        assertEquals("1", mapper.map("1"));
        assertEquals("2", mapper.map("2"));

        mapper = create(map, true, "A");

        assertEquals("A", mapper.map(null));
        assertEquals("A", mapper.map("1"));
        assertEquals("A", mapper.map("2"));
    }

    /**
     * Test regular mappings.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMappings() throws Exception {
        Map<String, String> map = new HashMap<>();

        map.put("1", "101");

        BasicUserNameMapper mapper = create(map, false, null);

        assertNull(mapper.map(null));
        assertEquals("101", mapper.map("1"));
        assertEquals("2", mapper.map("2"));

        mapper = create(map, true, null);

        assertNull(mapper.map(null));
        assertEquals("101", mapper.map("1"));
        assertNull(mapper.map("2"));

        mapper = create(map, false, "A");

        assertNull(mapper.map(null));
        assertEquals("101", mapper.map("1"));
        assertEquals("2", mapper.map("2"));

        mapper = create(map, true, "A");

        assertEquals("A", mapper.map(null));
        assertEquals("101", mapper.map("1"));
        assertEquals("A", mapper.map("2"));
    }

    /**
     * Create mapper.
     *
     * @param dictionary Dictionary.
     * @param useDfltUsrName Whether to use default user name.
     * @param dfltUsrName Default user name.
     * @return Mapper.
     */
    private BasicUserNameMapper create(@Nullable Map<String, String> dictionary, boolean useDfltUsrName,
        @Nullable String dfltUsrName) {
        BasicUserNameMapper mapper = new BasicUserNameMapper();

        mapper.setMappings(dictionary);
        mapper.setUseDefaultUserName(useDfltUsrName);
        mapper.setDefaultUserName(dfltUsrName);

        return mapper;
    }
}
