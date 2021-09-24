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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.definition.AbstractSchemaObject;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;
import org.apache.ignite.schema.definition.index.IndexColumnDefinition;

/**
 * Hash index.
 */
public class HashIndexDefinitionImpl extends AbstractSchemaObject implements HashIndexDefinition {
    /** Index columns. */
    @IgniteToStringInclude
    private final List<IndexColumnDefinition> columns;

    /**
     * Constructor.
     *
     * @param name Index name.
     * @param columns Index columns.
     */
    public HashIndexDefinitionImpl(String name, String[] columns) {
        super(name);

        this.columns = Arrays.stream(columns).map(IndexColumnDefinitionImpl::new).collect(Collectors.toUnmodifiableList());
    }


    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Override public List<IndexColumnDefinition> columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HashIndexDefinitionImpl.class, this,
            "type", type(),
            "name", name());
    }

}
