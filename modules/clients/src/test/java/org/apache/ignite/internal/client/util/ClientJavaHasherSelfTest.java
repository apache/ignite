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

package org.apache.ignite.internal.client.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for Java hash codes calculations - SHOULD BE PORTED to other languages.
 */
public class ClientJavaHasherSelfTest extends GridCommonAbstractTest {
    /**
     * Validate known Java hash codes.
     */
    public void testPredefined() {
        Map<Object, Integer> map = new LinkedHashMap<>();

        // Primitives.
        for (long i = 1, max = 1L << 48; i < max; i *= -3) {
            map.put((byte)i, (int)(byte)i);
            map.put((char)i, (int)(char)i);
            map.put((int)i, (int)i);
            map.put(i, (int)(i ^ (i >>> 32)));
        }

        // Double.
        map.put(0.0, 0);
        map.put(1.0, 1072693248);
        map.put(-1.0, -1074790400);
        map.put(3.1415e200, 1130072580);
        map.put(3.1415e-200, -819810675);

        // Strings
        map.put("", 0);
        map.put("asdf", 3003444);
        map.put("Hadoop\u3092\u6bba\u3059", 2113729932);
        map.put("224ea4cd-f449-4dcb-869a-5317c63bd619", 258755163);
        map.put("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd", -863611257);
        map.put("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636", -1499939567);
        map.put("d8f1f916-4357-4cfe-a7df-49d4721690bf", 2041432124);

        // UUID.
        map.put(UUID.fromString("224ea4cd-f449-4dcb-869a-5317c63bd619"), -1767478264);
        map.put(UUID.fromString("fdc9ec54-ff53-4fdb-8239-5a3ac1fb31bd"), 1096337416);
        map.put(UUID.fromString("0f9c9b94-02ae-45a6-9d5c-a066dbdf2636"), 1269913698);
        map.put(UUID.fromString("d8f1f916-4357-4cfe-a7df-49d4721690bf"), 1315925123);

        boolean ok = true;

        for (Map.Entry<Object, Integer> entry : map.entrySet()) {
            int act = entry.getKey().hashCode();
            int exp = entry.getValue();

            if (exp == act)
                continue;

            ok = false;

            info("Validation of hash code for '" + entry.getKey() + "' failed" +
                " [expected=" + exp + ", actual=" + act + ".");
        }

        if (ok)
            return;

        fail("Java hash codes validation fails.");
    }
}