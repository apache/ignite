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

package org.apache.ignite.internal.schema.definition.index;

import java.util.List;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.definition.index.PartialIndexDefinition;
import org.apache.ignite.schema.definition.index.SortedIndexColumnDefinition;

/**
 * Partial table index.
 */
public class PartialIndexDefinitionImpl extends SortedIndexDefinitionImpl implements PartialIndexDefinition {
    /** Expression. */
    private final String expr;

    /**
     * Constructor.
     *
     * @param name    Index name.
     * @param columns Index columns.
     * @param expr    Partial index expression.
     * @param unique  Unique flag.
     */
    public PartialIndexDefinitionImpl(String name, List<SortedIndexColumnDefinition> columns, String expr, boolean unique) {
        super(name, columns, unique);

        this.expr = expr;
    }

    /** {@inheritDoc} */
    @Override
    public String expr() {
        return expr;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(PartialIndexDefinition.class, this,
                "type", type(),
                "name", name(),
                "uniq", unique(),
                "cols", columns());
    }
}
