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

import org.apache.ignite.hadoop.util.KerberosUserNameMapper;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests for Kerberos name mapper.
 */
public class KerberosUserNameMapperSelfTest extends GridCommonAbstractTest {
    /** Test instance. */
    private static final String INSTANCE = "test_instance";

    /** Test realm. */
    private static final String REALM = "test_realm";

    /**
     * Test mapper without instance and realm components.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMapper() throws Exception {
        KerberosUserNameMapper mapper = create(null, null);

        assertEquals(IgfsUtils.fixUserName(null), mapper.map(null));
        assertEquals("test", mapper.map("test"));
    }

    /**
     * Test mapper with instance component.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMapperInstance() throws Exception {
        KerberosUserNameMapper mapper = create(INSTANCE, null);

        assertEquals(IgfsUtils.fixUserName(null) + "/" + INSTANCE, mapper.map(null));
        assertEquals("test" + "/" + INSTANCE, mapper.map("test"));
    }

    /**
     * Test mapper with realm.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMapperRealm() throws Exception {
        KerberosUserNameMapper mapper = create(null, REALM);

        assertEquals(IgfsUtils.fixUserName(null) + "@" + REALM, mapper.map(null));
        assertEquals("test" + "@" + REALM, mapper.map("test"));
    }

    /**
     * Test mapper with instance and realm components.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMapperInstanceAndRealm() throws Exception {
        KerberosUserNameMapper mapper = create(INSTANCE, REALM);

        assertEquals(IgfsUtils.fixUserName(null) + "/" + INSTANCE + "@" + REALM, mapper.map(null));
        assertEquals("test" + "/" + INSTANCE + "@" + REALM, mapper.map("test"));
    }

    /**
     * Create mapper.
     *
     * @param instance Instance.
     * @param realm Realm.
     * @return Mapper.
     */
    private KerberosUserNameMapper create(@Nullable String instance, @Nullable String realm) {
        KerberosUserNameMapper mapper = new KerberosUserNameMapper();

        mapper.setInstance(instance);
        mapper.setRealm(realm);

        mapper.start();

        return mapper;
    }
}
