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

import java.util.Set;
import org.apache.ignite.internal.schema.definition.AbstractSchemaObject;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.definition.PrimaryKeyDefinition;

/**
 * Primary key index.
 */
public class PrimaryKeyDefinitionImpl extends AbstractSchemaObject implements PrimaryKeyDefinition {
    /** Index columns. */
    @IgniteToStringInclude
    private final Set<String> columns;

    /** Affinity columns. */
    @IgniteToStringInclude
    private final Set<String> affCols;

    /**
     * Constructor.
     *
     * @param columns Index columns.
     * @param affinityColumns Affinity columns.
     */
    public PrimaryKeyDefinitionImpl(Set<String> columns, Set<String> affinityColumns) {
        super(PrimaryKeyDefinition.PRIMARY_KEY_NAME);

        this.columns = columns;
        this.affCols = affinityColumns;
    }

    /** {@inheritDoc} */
    @Override public Set<String> columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override public Set<String> affinityColumns() {
        return affCols;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PrimaryKeyDefinitionImpl.class, this,
            "name", name(),
            "cols", columns(),
            "affCols", affinityColumns());
    }
}
