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

package org.apache.ignite.jvmtest;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

/**
 * Test for {@link LinkedHashMap}.
 */
public class LinkedHashMapTest {
    /** @throws Exception If failed. */
    @Test
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
    @Test
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
    @Test
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
