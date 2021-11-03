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

import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.ResultFieldMetadata;
import org.apache.ignite.internal.schema.NativeType;

/**
 * Metadata of the field of a query result set.
 */
public class ResultFieldMetadataImpl implements ResultFieldMetadata {
    /** Name of the result's field. */
    private final String name;
    
    /** Type of the result's field. */
    private final NativeType type;
    
    /** Order of the result's field. */
    private final int order;
    
    /** Nullable flag of the result's field. */
    private final boolean nullable;
    
    /** Origin of the result's field. */
    private final List<String> origin;
    
    public ResultFieldMetadataImpl(
            String name,
            NativeType type,
            int order,
            boolean nullable,
            List<String> origin
    ) {
        this.name = name;
        this.type = type;
        this.order = order;
        this.nullable = nullable;
        this.origin = origin;
    }
    
    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }
    
    /** {@inheritDoc} */
    @Override
    public int order() {
        return order;
    }
    
    /** {@inheritDoc} */
    @Override
    public NativeType type() {
        return type;
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean isNullable() {
        return nullable;
    }
    
    /** {@inheritDoc} */
    @Override
    public List<String> origin() {
        return origin;
    }
}
