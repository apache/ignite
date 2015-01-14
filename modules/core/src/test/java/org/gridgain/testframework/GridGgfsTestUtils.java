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

package org.gridgain.testframework;

import net.sf.json.*;
import org.apache.ignite.*;

import java.util.*;

/**
 * Utility class for tests.
 */
public class GridGgfsTestUtils {
    /**
     * Converts json string to Map<String,String>.
     *
     * @param jsonStr String to convert.
     * @return Map.
     * @throws IgniteCheckedException If fails.
     */
    public static Map<String,String> jsonToMap(String jsonStr) throws IgniteCheckedException {
        Map<String,String> res = new HashMap<>();

        try {
            JSONObject jsonObj = JSONObject.fromObject(jsonStr);

            for (Object o : jsonObj.entrySet()) {
                Map.Entry e = (Map.Entry) o;

                res.put(e.getKey().toString(), e.getValue().toString());
            }

        }
        catch (JSONException e) {
            throw new IgniteCheckedException(e);
        }

        return res;
    }
}
