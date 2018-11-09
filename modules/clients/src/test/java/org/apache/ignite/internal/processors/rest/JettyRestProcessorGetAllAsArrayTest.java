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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.F;

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
