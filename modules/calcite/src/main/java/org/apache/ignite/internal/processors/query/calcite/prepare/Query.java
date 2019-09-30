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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class Query {
    private final String sql;
    private final Object[] params;

    public Query(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    public String sql() {
        return sql;
    }

    public Object[] params() {
        return params;
    }

    public Map<String, Object> params(Map<String, Object> stashed) {
        Map<String, Object> res = new HashMap<>(stashed);
        if (!F.isEmpty(params)) {
            for (int i = 0; i < params.length; i++) {
                res.put("?" + i, params[i]);
            }
        }
        return res;
    }
}
