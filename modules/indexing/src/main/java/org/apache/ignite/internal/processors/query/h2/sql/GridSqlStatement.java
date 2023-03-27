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

package org.apache.ignite.internal.processors.query.h2.sql;

/**
 * SQL statement to query or update grid caches.
 */
public abstract class GridSqlStatement {
    /** */
    protected GridSqlAst limit;

    /** */
    private boolean explain;

    /**
     * @return Generate sql.
     */
    public abstract String getSQL();

    /** {@inheritDoc} */
    @Override public String toString() {
        return getSQL();
    }

    /**
     * @param explain Explain.
     * @return {@code this}.
     */
    public GridSqlStatement explain(boolean explain) {
        this.explain = explain;

        return this;
    }

    /**
     * @return {@code true} If explain.
     */
    public boolean explain() {
        return explain;
    }

    /**
     * @param limit Limit.
     */
    public void limit(GridSqlAst limit) {
        this.limit = limit;
    }

    /**
     * @return Limit.
     */
    public GridSqlAst limit() {
        return limit;
    }
}
