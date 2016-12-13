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

import java.util.Collections;
import org.h2.command.Parser;
import org.h2.expression.ValueExpression;

/** Keyword (like DEFAULT). */
public final class GridSqlKeyword extends GridSqlElement {
    /**
     * Default update value - analogous to H2.
     * @see ValueExpression#getDefault()
     * @see Parser#parseUpdate()
     */
    public final static GridSqlKeyword DEFAULT = new GridSqlKeyword("DEFAULT");

    /** Keyword to return as SQL. */
    private final String keyword;

    /** */
    private GridSqlKeyword(String keyword) {
        super(Collections.<GridSqlElement>emptyList());
        this.keyword = keyword;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return keyword;
    }
}
