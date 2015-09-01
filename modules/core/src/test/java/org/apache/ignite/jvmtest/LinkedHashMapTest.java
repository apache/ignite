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

package org.apache.ignite.jvmtest;

import java.util.LinkedHashMap;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Test for {@link LinkedHashMap}.
 */
public class LinkedHashMapTest extends TestCase {
    /** @throws Exception If failed. */
    public void testAccessOrder1() throws Exception {
        X.println(">>> testAccessOrder1 <<<");

        Map<String, String> map = new LinkedHashMap<>(3, 0.75f, true);

        for (int i = 1; i <= 3; i++)
            map.put("k" + i, "v" + i);

        X.println("Initial state: " + map);

        int i = 0;

        for (Map.Entry<String, String> entry : map.entrySet()) {
            X.println("Entry: " + entry);

            if (i > 1)
                break;

            i++;
        }

        X.println("State after loop: " + map);
    }

    /** @throws Exception If failed. */
    public void testAccessOrder2() throws Exception {
        X.println(">>> testAccessOrder2 <<<");

        Map<String, String> map = new LinkedHashMap<>(3, 0.75f, true);

        for (int i = 1; i <= 3; i++)
            map.put("k" + i, "v" + i);

        X.println("Initial state: " + map);

        // Accessing second entry.
        map.get("k2");

        X.println("State after get: " + map);
    }

    /** @throws Exception If failed. */
    public void testAccessOrder3() throws Exception {
        X.println(">>> testAccessOrder3 <<<");

        Map<String, String> map = new LinkedHashMap<>(3, 0.75f, true);

        map.put("k1", "v1");
        map.put("k2", "v2");

        X.println("Initial state: " + map);

        // Accessing first entry.
        map.get("k1");

        X.println("State after get: " + map);
    }
}