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

package org.apache.ignite.internal.schema.definition.builder;

import java.util.Map;
import org.apache.ignite.internal.schema.definition.index.HashIndexDefinitionImpl;
import org.apache.ignite.schema.definition.builder.HashIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.index.HashIndexDefinition;

/**
 * Hash index builder.
 */
public class HashIndexDefinitionBuilderImpl extends AbstractIndexBuilder implements HashIndexDefinitionBuilder {
    /** Index columns. */
    private String[] columns;

    /**
     * Constructor.
     *
     * @param name Index name.
     */
    public HashIndexDefinitionBuilderImpl(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override public HashIndexDefinitionBuilder withColumns(String... columns) {
        this.columns = columns.clone();

        return this;
    }

    /** {@inheritDoc} */
    @Override public HashIndexDefinitionBuilderImpl withHints(Map<String, String> hints) {
        super.withHints(hints);

        return this;
    }

    /** {@inheritDoc} */
    @Override public HashIndexDefinition build() {
        assert columns != null;
        assert columns.length > 0;

        return new HashIndexDefinitionImpl(name, columns);
    }
}
