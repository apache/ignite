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
 * Utility methods for splitter.
 */
public class SplitterUtils {
    /**
     * Check whether AST element has aggregates.
     *
     * @param el Expression part in SELECT clause.
     * @return {@code true} If expression contains aggregates.
     */
    public static boolean hasAggregates(GridSqlAst el) {
        if (el instanceof GridSqlAggregateFunction)
            return true;

        // If in SELECT clause we have a subquery expression with aggregate,
        // we should not split it. Run the whole subquery on MAP stage.
        if (el instanceof GridSqlSubquery)
            return false;

        for (int i = 0; i < el.size(); i++) {
            if (hasAggregates(el.child(i)))
                return true;
        }

        return false;
    }

    /**
     * Private constructor.
     */
    private SplitterUtils() {
        // No-op.
    }
}
