/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.impl.util;

import org.apache.ignite.hadoop.util.KerberosUserNameMapper;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Kerberos name mapper.
 */
@RunWith(JUnit4.class)
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
