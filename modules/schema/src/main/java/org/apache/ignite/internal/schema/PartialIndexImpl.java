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

package org.apache.ignite.internal.schema;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.SortedIndexColumn;

/**
 * Partial table index.
 */
public class PartialIndexImpl extends SortedIndexImpl implements PartialIndex {
    /** Expression. */
    private final String expr;

    /**
     * Constructor.
     *
     * @param name Index name.
     * @param columns Index columns.
     * @param uniq Unique flag.
     * @param expr Partial index expression.
     */
    public PartialIndexImpl(String name, List<SortedIndexColumn> columns, boolean uniq, String expr) {
        super(name, columns, uniq);

        this.expr = expr;
    }

    /** {@inheritDoc} */
    @Override public String expr() {
        return expr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "PartialIndex[" +
            "name='" + name() + '\'' +
            ", type=PARTIAL" +
            ", expr='" + expr + '\'' +
            ", columns=[" + columns().stream().map(IndexColumn::toString).collect(Collectors.joining(",")) + ']' +
            ']';
    }
}
