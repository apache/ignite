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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.IndexColumn;

/**
 * Hash index.
 */
public class HashIndexImpl extends AbstractSchemaObject implements HashIndex {
    /** Index columns. */
    @IgniteToStringInclude
    private final List<IndexColumn> columns;

    /**
     * Constructor.
     *
     * @param name Index name.
     * @param columns Index columns.
     */
    public HashIndexImpl(String name, String[] columns) {
        super(name);

        this.columns = Arrays.stream(columns).map(IndexColumnImpl::new).collect(Collectors.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Override public List<IndexColumn> columns() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HashIndexImpl.class, this,
            "type", type(),
            "name", name());
    }

}
