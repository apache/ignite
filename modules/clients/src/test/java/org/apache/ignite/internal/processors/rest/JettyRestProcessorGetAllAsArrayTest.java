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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REST_GETALL_AS_ARRAY;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/** */
public class JettyRestProcessorGetAllAsArrayTest extends JettyRestProcessorCommonSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_REST_GETALL_AS_ARRAY, "true");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_REST_GETALL_AS_ARRAY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAll() throws Exception {
        final Map<String, String> entries = F.asMap("getKey1", "getVal1", "getKey2", "getVal2");

        jcache().putAll(entries);

        String ret = content(DEFAULT_CACHE_NAME, GridRestCommand.CACHE_GET_ALL,
            "k1", "getKey1",
            "k2", "getKey2"
        );

        info("Get all command result: " + ret);

        assertNotNull(ret);
        assertFalse(ret.isEmpty());

        JsonNode node = JSON_MAPPER.readTree(ret);

        assertEquals(STATUS_SUCCESS, node.get("successStatus").asInt());
        assertTrue(node.get("error").isNull());

        JsonNode res = node.get("response");

        assertTrue(res.isArray());

        Set<Map<String, String>> returnValue = new HashSet<>();

        returnValue.add(F.asMap("key", "getKey1", "value", "getVal1"));
        returnValue.add(F.asMap("key", "getKey2", "value", "getVal2"));

        assertEquals(returnValue, JSON_MAPPER.treeToValue(res, Set.class));
    }

    /** {@inheritDoc} */
    @Override protected String signature() {
        return null;
    }
}
