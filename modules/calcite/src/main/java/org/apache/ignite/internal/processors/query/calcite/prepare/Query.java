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

import java.util.Arrays;

/**
 * Represents a query and its parameters.
 */
public class Query {
    /** */
    private final String sql;

    /** */
    private final Object[] params;

    /**
     * @param sql Query text.
     * @param params Query parameters.
     */
    public Query(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    /**
     * @return Query text.
     */
    public String sql() {
        return sql;
    }

    /**
     * @return Query parameters.
     */
    public Object[] parameters() {
        return params;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Query))
            return false;

        Query query = (Query) o;

        if (!sql.equals(query.sql))
            return false;
        return Arrays.equals(params, query.params);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = sql.hashCode();
        result = 31 * result + Arrays.hashCode(params);
        return result;
    }
}
